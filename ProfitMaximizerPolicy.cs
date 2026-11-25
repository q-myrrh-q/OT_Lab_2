using SPS.Abstractions;

namespace SPS.Student;

/// <summary>
/// Стратегия максимизации Profit. Все вспомогательные классы (включая RPC-политику из лабораторной 1)
/// лежат внутри этого файла; зависимость только от SPS.Abstractions.
/// Кратко:
///  - Generate: задания выбираются по "ценности за секунду воркера" с учётом вероятности, что клиент ещё ждёт;
///    безнадёжные отклоняются; парк воркеров масштабируется по входящему потоку и очереди.
///  - Metadata/Preview: кэш с учётом требования свежести каждого клиента, объединение загрузок,
///    вытеснение по популярности ключей.
///  - API держим минимально (1 + запасной при сбоях), здоровье экземпляров определяем по результатам вызовов и пробам.
///  - Восстановление заданий по heartbeat и пробам воркера.
/// </summary>
public sealed class ProfitMaximizerPolicy : ISystemPolicy
{
    // ---------- Известные диапазоны терпения клиентов (секунды) ----------
    private const double GenPatienceMin = 15, GenPatienceMax = 45;
    private const double MetaPatienceMax = 1.5, PrevPatienceMax = 3.0;

    // ---------- Настройки ----------
    private const double WorkerUtilization = 1.0;      // целевая загрузка воркеров по входящему потоку
    private const double QueueDrainHorizon = 15.0;     // за сколько секунд хотим разгрести очередь
    private const int ScaleDownStreak = 2;             // сколько обзоров подряд нужно "избыточности"
    private const double RateTau = 10.0;               // постоянная времени оценки интенсивности Generate
    private const double PopularityTau = 8.0;          // постоянная времени оценки популярности ключей
    private const double HeartbeatTimeout = 1.5;       // тишина воркера, после которой он подозрителен
    private const double JobSilenceTimeout = 1.4;      // тишина по конкретному заданию
    private const double RejectSurvival = 0.02;        // ниже этой вероятности задание отклоняем

    private readonly RpcImpl rpc;
    public IRequestPolicy RpcPolicy => rpc;

    public ProfitMaximizerPolicy() { rpc = new RpcImpl(this); }

    // ---------- Общее состояние ----------
    private TimeSpan now;
    private RequestKind hintKind = RequestKind.Preview;
    private readonly Decay genArrivals = new(RateTau);
    private double meanGenWork = 3.0;
    private bool haveGenWork;
    private readonly double[] maxAgeSeen = new double[3];
    private readonly Dictionary<string, Decay> popularity = new(StringComparer.Ordinal);
    private readonly Dictionary<long, InstState> inst = new();
    private int lowStreak;

    private enum Health { Healthy, Suspect, Dead, HeartbeatBlind }

    private sealed class InstState
    {
        public double ReadyAt;
        public Health Health = Health.Healthy;
        public double ProbeSince = -1;
        public double AllowSince;
        public double QuarantineUntil;
        public double BadRate;
    }

    private InstState St(long id)
    {
        if (!inst.TryGetValue(id, out var s)) inst[id] = s = new InstState();
        return s;
    }

    private sealed class Decay
    {
        private readonly double tau;
        private double value, at;
        public Decay(double tau) { this.tau = tau; }
        public double Value(double t) => value * Math.Exp(-(t - at) / tau);
        public void Add(double t, double w = 1) { value = Value(t) + w; at = t; }
    }

    // ---------- Приход запроса (GetProcessingBudget вызывается один раз на запрос) ----------
    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random)
    {
        double t = request.CreatedAt.TotalSeconds;
        now = request.CreatedAt;
        int k = (int)request.Kind;
        maxAgeSeen[k] = Math.Max(maxAgeSeen[k], request.MaxDataAge.TotalSeconds);
        if (request.Kind == RequestKind.Generate)
        {
            genArrivals.Add(t);
            double w = request.EstimatedWorkSeconds;
            meanGenWork = haveGenWork ? meanGenWork * 0.97 + w * 0.03 : w;
            haveGenWork = true;
            // Страховка от мёртвых заданий в очереди: выполняющихся заданий такого возраста мы не допускаем.
            return TimeSpan.FromSeconds(62);
        }
        if (!popularity.TryGetValue(request.ResourceKey, out var d)) popularity[request.ResourceKey] = d = new Decay(PopularityTau);
        d.Add(t);
        // Дольше терпения клиента обрабатывать смысла нет.
        return TimeSpan.FromSeconds(request.Kind == RequestKind.Metadata ? MetaPatienceMax + 0.15 : PrevPatienceMax + 0.2);
    }

    // ---------- Кэш ----------
    public CacheReadDecision OnCacheRead(CacheReadContext c, Random random)
    {
        now = c.Now;
        var req = c.Request;
        double maxAge = req.MaxDataAge.TotalSeconds;
        if (c.Entry is { } e && (c.Now - e.SourceObservedAt).TotalSeconds + 0.01 <= maxAge)
            return CacheReadDecision.ServeCached();

        double joinWindow = req.Kind == RequestKind.Metadata ? 0.35 : 0.8;
        FetchInfo? best = null;
        foreach (var f in c.PendingFetches)
        {
            double age = (c.Now - f.StartedAt).TotalSeconds;
            if (age <= joinWindow && (best == null || f.StartedAt < best.StartedAt)) best = f;
        }
        if (best != null) return CacheReadDecision.JoinFetch(best.Id);
        hintKind = req.Kind;
        return CacheReadDecision.StartFetch();
    }

    private static int KindOfKey(string key) => key.StartsWith("Metadata", StringComparison.Ordinal) ? 0 : 1;

    private double Rate(string key, double t) => popularity.TryGetValue(key, out var d) ? d.Value(t) : 0;

    public CacheWriteDecision OnCacheFill(CacheFillContext c, Random random)
    {
        now = c.Now;
        double t = c.Now.TotalSeconds;
        var v = c.Value;
        int kind = KindOfKey(v.Key);
        double life = maxAgeSeen[kind] - (c.Now - v.SourceObservedAt).TotalSeconds - 0.02;
        if (life <= 0.05 || v.SizeBytes > c.CapacityBytes) return CacheWriteDecision.Skip();
        var ttl = TimeSpan.FromSeconds(life);

        long others = 0;
        var candidates = new List<CacheEntryInfo>();
        foreach (var e in c.Entries)
        {
            if (e.Key == v.Key) continue;
            others += e.SizeBytes;
            candidates.Add(e);
        }
        if (others + v.SizeBytes <= c.CapacityBytes) return CacheWriteDecision.Retain(ttl);

        // Вытесняем наименее полезные: популярность на байт.
        double newDensity = Rate(v.Key, t) / v.SizeBytes;
        candidates.Sort((a, b) => (Rate(a.Key, t) / a.SizeBytes).CompareTo(Rate(b.Key, t) / b.SizeBytes));
        var evict = new List<string>();
        foreach (var e in candidates)
        {
            if (others + v.SizeBytes <= c.CapacityBytes) break;
            if (Rate(e.Key, t) / e.SizeBytes > newDensity) return CacheWriteDecision.Skip();
            evict.Add(e.Key);
            others -= e.SizeBytes;
        }
        if (others + v.SizeBytes > c.CapacityBytes) return CacheWriteDecision.Skip();
        return CacheWriteDecision.Retain(ttl, evict.ToArray());
    }

    // ---------- Маршрутизация на API ----------
    public EndpointSelectionDecision? SelectEndpoint(RoutingContext c, Random random)
    {
        now = c.Now;
        EndpointInfo? best = null;
        double bestScore = double.MaxValue;
        foreach (var e in c.Endpoints)
        {
            double score = (double)e.OutstandingCalls / Math.Max(1, e.Slots) + St(e.Id).BadRate * 10 - e.Speed * 0.01;
            if (score < bestScore) { bestScore = score; best = e; }
        }
        if (best == null) return null;
        hintKind = c.Request.Kind;
        return EndpointSelectionDecision.Assign(best.Id);
    }

    // ---------- Здоровье экземпляров ----------
    public EndpointDecision ReviewEndpoint(EndpointContext c, Random random)
    {
        now = c.Now;
        var e = c.Endpoint;
        return e.Role == InstanceRole.Api ? ReviewApi(c) : ReviewWorker(c);
    }

    private EndpointDecision ReviewApi(EndpointContext c)
    {
        var e = c.Endpoint;
        double t = c.Now.TotalSeconds;
        var st = St(e.Id);
        if (e.State == InstanceState.Draining) return EndpointDecision.Allow();

        double since = Math.Max(t - 2.0, st.AllowSince);
        int good = 0, bad = 0;
        double lastProbe = -1; bool probeOk = false;
        foreach (var r in c.RecentResults)
        {
            double rt = r.Time.TotalSeconds;
            if (r.IsProbe)
            {
                if (rt >= st.ProbeSince && rt > lastProbe) { lastProbe = rt; probeOk = r.Outcome is RequestOutcome.Success or RequestOutcome.Rejected; }
                continue;
            }
            if (rt < since) continue;
            if (r.Outcome == RequestOutcome.Success) good++;
            else if (r.Outcome is RequestOutcome.Timeout or RequestOutcome.Unavailable) bad++;
        }
        st.BadRate = good + bad == 0 ? 0 : (double)bad / (good + bad);

        if (e.Mode == EndpointMode.AllowTraffic)
        {
            if (bad >= 3 && st.BadRate >= 0.6)
            {
                st.ProbeSince = t;
                return EndpointDecision.Probe();
            }
            return EndpointDecision.Allow();
        }
        // Probe/Block: возвращаем трафик после успешной пробы.
        if (lastProbe >= 0 && probeOk && t - lastProbe < 1.0)
        {
            st.AllowSince = t;
            st.BadRate = 0;
            return EndpointDecision.Allow();
        }
        return EndpointDecision.Probe();
    }

    private EndpointDecision ReviewWorker(EndpointContext c)
    {
        var e = c.Endpoint;
        double t = c.Now.TotalSeconds;
        var st = St(e.Id);
        if (e.State == InstanceState.Draining) return EndpointDecision.Allow();

        bool fresh = e.LastHeartbeat is { } hb && t - hb.TotalSeconds <= HeartbeatTimeout;
        if (!fresh && e.LastHeartbeat == null && t - st.ReadyAt <= 2.0) fresh = true;

        if (fresh)
        {
            if (st.Health != Health.Healthy) st.AllowSince = t;
            st.Health = Health.Healthy;
            return EndpointDecision.Allow();
        }

        // Heartbeat пропал: проба различает падение/изоляцию и потерю только heartbeat.
        if (e.Mode == EndpointMode.AllowTraffic && st.Health != Health.HeartbeatBlind)
        {
            st.Health = Health.Suspect;
            st.ProbeSince = t;
            return EndpointDecision.Probe();
        }
        double lastProbe = -1; bool probeOk = false;
        foreach (var r in c.RecentResults)
        {
            double rt = r.Time.TotalSeconds;
            if (r.IsProbe && rt >= st.ProbeSince && rt > lastProbe) { lastProbe = rt; probeOk = r.Outcome is RequestOutcome.Success or RequestOutcome.Rejected; }
        }
        if (lastProbe >= 0 && t - lastProbe < 1.2)
        {
            if (probeOk)
            {
                st.Health = Health.HeartbeatBlind;
                return EndpointDecision.Allow();
            }
            st.Health = Health.Dead;
        }
        if (st.Health == Health.HeartbeatBlind)
        {
            // Периодически перепроверяем, что воркер всё ещё отвечает.
            if (t - Math.Max(lastProbe, st.ProbeSince) > 3.0) { st.ProbeSince = t; st.Health = Health.Suspect; return EndpointDecision.Probe(); }
            return EndpointDecision.Allow();
        }
        if (st.Health != Health.Dead) st.Health = Health.Suspect;
        return EndpointDecision.Probe();
    }

    // ---------- Задания ----------
    private static double Survival(double ageAtFinish)
    {
        if (ageAtFinish <= GenPatienceMin) return 1;
        if (ageAtFinish >= GenPatienceMax) return 0;
        return (GenPatienceMax - ageAtFinish) / (GenPatienceMax - GenPatienceMin);
    }

    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext c, Random random)
    {
        now = c.Now;
        double t = c.Now.TotalSeconds;
        EndpointInfo? worker = null;
        foreach (var w in c.AvailableWorkers)
        {
            var st = St(w.Id);
            if (st.QuarantineUntil > t || st.Health is Health.Dead or Health.Suspect) continue;
            if (worker == null || w.Speed > worker.Speed) worker = w;
        }
        if (worker == null || c.QueuedJobs.Count == 0) return null;

        JobInfo? best = null;
        double bestScore = -1;
        foreach (var job in c.QueuedJobs)
        {
            double age = t - job.Request.CreatedAt.TotalSeconds;
            double work = Math.Max(0.2, job.Request.EstimatedWorkSeconds) / worker.Speed;
            double s = Survival(age + work + 0.3);
            if (s < RejectSurvival) return JobAssignmentDecision.Reject(job.Id);
            double score = (double)job.Request.Revenue * s / work;
            if (score > bestScore) { bestScore = score; best = job; }
        }
        return best == null ? null : JobAssignmentDecision.Assign(best.Id, worker.Id);
    }

    public JobRecoveryDecision ReviewJob(JobRecoveryContext c, Random random)
    {
        now = c.Now;
        var j = c.Job;
        if (j.WorkerId is not { } wid || j.AssignedAt is not { } assignedAt) return JobRecoveryDecision.KeepWaiting();
        double t = c.Now.TotalSeconds;
        var st = St(wid);
        double lastSignal = (j.LastHeartbeat ?? assignedAt).TotalSeconds;
        double silence = t - lastSignal;
        double slowest = Math.Max(0.5, j.Request.EstimatedWorkSeconds) / 0.8;

        if (st.Health == Health.Dead) return Requeue(st, t, slowest);
        if (silence <= JobSilenceTimeout) return JobRecoveryDecision.KeepWaiting();

        switch (st.Health)
        {
            case Health.Healthy:
                // Воркер жив и шлёт heartbeat, а о задании молчит: назначение потерялось.
                return Requeue(st, t, slowest);
            case Health.HeartbeatBlind:
                return t - assignedAt.TotalSeconds > slowest * 1.6 + 3.0 ? Requeue(st, t, slowest) : JobRecoveryDecision.KeepWaiting();
            default:
                return JobRecoveryDecision.KeepWaiting();
        }
    }

    private static JobRecoveryDecision Requeue(InstState st, double t, double slowest)
    {
        // Старое выполнение могло продолжиться, не назначаем туда новые задания пока оно не завершится.
        st.QuarantineUntil = Math.Max(st.QuarantineUntil, t + Math.Min(slowest * 1.3, 8.0));
        return JobRecoveryDecision.RequeueAfter(TimeSpan.Zero);
    }

    // ---------- Ёмкость ----------
    public CapacityPlan ReviewCapacity(CapacityContext c, Random random)
    {
        now = c.Now;
        double t = c.Now.TotalSeconds;
        var s = c.Settings;

        int readyApis = 0, unhealthyApis = 0, ownedWorkers = 0;
        double speedSum = 0; int speedN = 0;
        foreach (var i in c.Instances)
        {
            if (i.Role == InstanceRole.Api)
            {
                if (i.State == InstanceState.Ready) { readyApis++; if (i.Mode != EndpointMode.AllowTraffic) unhealthyApis++; }
            }
            else if (i.State is InstanceState.Ready or InstanceState.Starting)
            {
                ownedWorkers++;
                speedSum += i.Speed; speedN++;
            }
        }
        double avgSpeed = speedN > 0 ? speedSum / speedN : 1;

        int apiForLoad = (int)Math.Ceiling(c.PendingCalls / (s.Api.Slots * 1.5));
        int desiredApi = Math.Max(1, apiForLoad) + unhealthyApis;
        desiredApi = Math.Min(desiredApi, s.MaximumApis);

        double lambda = genArrivals.Value(t) / RateTau;
        double rho = lambda * meanGenWork / avgSpeed;
        double queueWork = 0;
        foreach (var job in c.QueuedJobs)
        {
            double age = t - job.Request.CreatedAt.TotalSeconds;
            double w = job.Request.EstimatedWorkSeconds / avgSpeed;
            queueWork += w * Survival(age + w + QueueDrainHorizon / 2);
        }
        double target = rho / WorkerUtilization + queueWork / QueueDrainHorizon;
        int desiredWorkers = Math.Max(1, (int)Math.Ceiling(target - 1e-9));

        int maxWorkers = Math.Min(s.MaximumWorkers, (s.ComputeUnitLimit - desiredApi * s.Api.ComputeUnits) / Math.Max(1, s.Worker.ComputeUnits));
        desiredWorkers = Math.Min(desiredWorkers, Math.Max(1, maxWorkers));

        List<long>? retire = null;
        if (desiredWorkers < ownedWorkers)
        {
            desiredWorkers = ownedWorkers;
            if (++lowStreak >= ScaleDownStreak)
            {
                // Сворачиваем только простаивающий готовый воркер, самый медленный; запускающиеся не трогаем.
                EndpointInfo? victim = null;
                foreach (var i in c.Instances)
                {
                    if (i.Role != InstanceRole.Worker || i.State != InstanceState.Ready || i.AssignedJobs != 0) continue;
                    if (victim == null || i.Speed < victim.Speed) victim = i;
                }
                if (victim != null) { retire = new List<long> { victim.Id }; desiredWorkers = ownedWorkers - 1; lowStreak = 0; }
            }
        }
        else lowStreak = 0;

        // Если не помещаемся по вычислительным единицам, уменьшаем число API до минимума.
        while (desiredApi > 1 && desiredApi * s.Api.ComputeUnits + desiredWorkers * s.Worker.ComputeUnits > s.ComputeUnitLimit) desiredApi--;
        while (desiredWorkers > 1 && desiredApi * s.Api.ComputeUnits + desiredWorkers * s.Worker.ComputeUnits > s.ComputeUnitLimit) desiredWorkers--;
        return new CapacityPlan(desiredApi, desiredWorkers, retire);
    }

    // ---------- Наблюдения ----------
    public void Observe(SystemObservation o)
    {
        now = o.Now;
        switch (o.Kind)
        {
            case ObservationKind.InstanceReady when o.EndpointId is { } id:
                St(id).ReadyAt = o.Now.TotalSeconds;
                break;
            case ObservationKind.InstanceStopped when o.EndpointId is { } stopped:
                inst.Remove(stopped);
                break;
        }
    }

    // ================= RPC-политика (лабораторная 1, адаптированная) =================
    private sealed class RpcImpl : IRequestPolicy
    {
        private const double Quantile = 0.95, Margin = 1.4, Censored = 3.0, Growth = 1.5;
        private static readonly double[] InitialMs = { 150, 250, 250 };
        private static readonly double[] MinMs = { 50, 80, 100 };
        private static readonly double[] MaxMs = { 800, 1500, 6000 };
        private static readonly double[] PatienceMs = { MetaPatienceMax * 1000, PrevPatienceMax * 1000, 40_000 };
        private static readonly int[] MaxAttempts = { 6, 8, 40 };
        private static readonly double[] TokensPerRequest = { 0.6, 0.6, 3.0 };

        private readonly ProfitMaximizerPolicy owner;
        private readonly Window[] windows = { new(), new(), new() };
        private readonly double[] tokens = { 15, 15, 40 };
        private readonly Dictionary<long, RpcState> states = new();

        public RpcImpl(ProfitMaximizerPolicy owner) { this.owner = owner; }

        private sealed class RpcState { public int Kind; public double CumMs; }

        private sealed class Window
        {
            private const int Size = 200, MinSamples = 20;
            private readonly double[] data = new double[Size];
            private readonly double[] scratch = new double[Size];
            private int count, next;
            public double? Estimate;
            public void Add(double ms)
            {
                data[next] = ms; next = (next + 1) % Size;
                if (count < Size) count++;
                if (count < MinSamples) return;
                Array.Copy(data, scratch, count);
                Array.Sort(scratch, 0, count);
                int idx = Math.Min(count - 1, (int)Math.Ceiling(count * Quantile) - 1);
                Estimate = scratch[idx] * Margin;
            }
        }

        public TimeSpan GetTimeout(RequestContext context, Random random)
        {
            if (!states.TryGetValue(context.RequestId, out var st))
            {
                st = new RpcState { Kind = (int)owner.hintKind };
                states[context.RequestId] = st;
                tokens[st.Kind] = Math.Min(60, tokens[st.Kind] + TokensPerRequest[st.Kind]);
            }
            int k = st.Kind;
            double est = windows[k].Estimate ?? InitialMs[k];
            double ms = est * Math.Pow(Growth, Math.Max(0, context.Attempt - 1));
            ms = Math.Clamp(ms, MinMs[k], MaxMs[k]);
            return TimeSpan.FromMilliseconds(ms);
        }

        public RetryDecision OnResult(RequestObservation obs, Random random)
        {
            if (!states.TryGetValue(obs.RequestId, out var st)) return RetryDecision.NoRetry();
            int k = st.Kind;
            double ms = obs.Elapsed.TotalMilliseconds;
            if (obs.Outcome == RequestOutcome.Success)
            {
                windows[k].Add(ms);
                states.Remove(obs.RequestId);
                return RetryDecision.NoRetry();
            }
            if (obs.Outcome == RequestOutcome.Timeout) windows[k].Add(ms * Censored);
            st.CumMs += ms;
            bool retriable = obs.Outcome is RequestOutcome.Timeout or RequestOutcome.Rejected or RequestOutcome.Unavailable;
            if (!retriable || obs.Attempt >= MaxAttempts[k] || st.CumMs >= PatienceMs[k] || tokens[k] < 1)
            {
                states.Remove(obs.RequestId);
                return RetryDecision.NoRetry();
            }
            tokens[k] -= 1;
            double delay;
            if (k == 2)
            {
                double cap = Math.Min(2000, 100 * Math.Pow(2, Math.Min(obs.Attempt - 1, 5)));
                delay = obs.Outcome == RequestOutcome.Timeout ? cap * (0.5 + 0.5 * random.NextDouble()) : 300 + 700 * random.NextDouble();
            }
            else delay = obs.Outcome == RequestOutcome.Timeout ? 25 * random.NextDouble() : 40 + 80 * random.NextDouble();
            st.CumMs += delay;
            return RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(delay));
        }
    }
}
