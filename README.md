# Siamese Resonance Dashboard

A highly resilient, real-time telemetry dashboard deployed to Azure East US (`rg-siamese-resonance`). This system visualizes streaming resonance data by combining a Holonic multi-agent architecture, strictly ordered Azure Service Bus messaging, and graph-validated Attribute-Based Access Control (ABAC).

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Holonic Multi-Agent Observer Pattern](#holonic-multi-agent-observer-pattern)
3. [Apex SignalR Integration](#apex-signalr-integration)
4. [Strict FIFO & Resilience](#strict-fifo--resilience)
5. [Graph-Validated ABAC Pipeline](#graph-validated-abac-pipeline)
6. [Human-in-the-Loop (HITL) Workflows](#human-in-the-loop-hitl-workflows)

---

## Architecture Overview

The backend is built on **C#** and **ASP.NET Core** utilizing Clean Architecture / Onion Architecture principles to strictly decouple domain logic from application and presentation concerns.

* **Frontend:** ASP.NET Blazor Server (Razor Class Libraries)
* **Hosting:** Azure Container Apps (`rg-siamese-resonance` in East US)
* **Real-Time Transport:** ASP.NET Core SignalR
* **Messaging:** Azure Service Bus (`ServiceBusSessionProcessor` for sessionful FIFO ordering)
* **Access Control:** Two-phase gate combining SHACL graph validation (`dotNetRDF`) with OPA / Cedar policy evaluation
* **Persistence:** Hybrid normalized relational and document-oriented stores

---

## Holonic Multi-Agent Observer Pattern

Instead of a flat Observer implementation, the real-time notification engine utilizes a recursive **Holonic multi-agent system**. Each agent (a "Holon") acts as both an observer of lower-level telemetry and a subject to higher-level aggregators, making autonomous localized decisions via the GoF Strategy pattern before propagating events upward.

### Core Interface Contracts

```csharp
public interface IHolonicAgent<TResonanceEvent> : 
    IObservable<TResonanceEvent>, 
    IObserver<TResonanceEvent>
{
    Guid AgentId { get; }
    HolonLevel Level { get; }
    
    void AttachChild(IHolonicAgent<TResonanceEvent> child);
    void DetachChild(IHolonicAgent<TResonanceEvent> child);
}
```

### Strategy-Driven Autonomous Evaluation

When a parent holon receives an `OnNext` notification from a child, it executes its injected `IResonanceEvaluationStrategy` to determine whether the resonance event is an anomaly requiring broader broadcast or normal localized variance that can be absorbed.

```csharp
public class ResonanceHolon : IHolonicAgent<ResonanceTelemetry>
{
    private readonly List<IObserver<ResonanceTelemetry>> _parents = new();
    private readonly List<IHolonicAgent<ResonanceTelemetry>> _children = new();
    private readonly IResonanceEvaluationStrategy _strategy;

    public ResonanceHolon(IResonanceEvaluationStrategy strategy)
    {
        _strategy = strategy;
    }

    public void OnNext(ResonanceTelemetry telemetry)
    {
        var synthesizedState = _strategy.Evaluate(telemetry, _children.Select(c => c.CurrentState));

        if (synthesizedState.RequiresPropagation)
        {
            NotifyParents(synthesizedState);
        }
    }

    public IDisposable Subscribe(IObserver<ResonanceTelemetry> observer)
    {
        if (!_parents.Contains(observer))
            _parents.Add(observer);
            
        return new Unsubscriber(_parents, observer);
    }

    private void NotifyParents(ResonanceTelemetry state)
    {
        foreach (var parent in _parents)
        {
            parent.OnNext(state);
        }
    }

    public void OnError(Exception error) { /* Handled via Polly resilience pipeline */ }
    public void OnCompleted() { /* Lifecycle cleanup */ }
}
```

---

## Apex SignalR Integration

Sitting at the apex of the hierarchy, the **Root Holon** bridges the domain's reactive event streams and the ASP.NET Core infrastructure layer. Instead of notifying a parent agent, it injects a strongly-typed SignalR `IHubContext` and broadcasts synthesized system-wide telemetry directly to connected Blazor Server clients.

```csharp
public interface IResonanceDashboardClient
{
    Task ReceiveApexTelemetryAsync(ResonanceTelemetry telemetry);
    Task ReceiveSystemStateChangeAsync(ResonanceSystemState state);
}

public class ApexResonanceHolon : IObserver<ResonanceTelemetry>
{
    private readonly IHubContext<ResonanceHub, IResonanceDashboardClient> _hubContext;
    private readonly ILogger<ApexResonanceHolon> _logger;

    public ApexResonanceHolon(
        IHubContext<ResonanceHub, IResonanceDashboardClient> hubContext,
        ILogger<ApexResonanceHolon> logger)
    {
        _hubContext = hubContext;
        _logger = logger;
    }

    public void OnNext(ResonanceTelemetry telemetry)
    {
        try
        {
            _hubContext.Clients.All.ReceiveApexTelemetryAsync(telemetry);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to broadcast telemetry via SignalR.");
        }
    }

    public void OnError(Exception error)
    {
        _logger.LogCritical(error, "Critical failure propagating up the Holonic tree.");
        _hubContext.Clients.All.ReceiveSystemStateChangeAsync(ResonanceSystemState.Degraded);
    }

    public void OnCompleted()
    {
        _logger.LogInformation("Telemetry stream completed.");
    }
}
```

---

## Strict FIFO & Resilience

To guarantee strict First-In-First-Out (FIFO) ordering across the Leaf Holons:

1. **Sessionful Ingestion:** Background services (`IHostedService`) running in Azure Container Apps consume telemetry via `ServiceBusSessionProcessor` from the `Azure.Messaging.ServiceBus` SDK, locking session states to specific sensor arrays.
2. **Secret Management:** Connection strings and signing certificates are dynamically resolved via Azure Key Vault integration.
3. **Fault Tolerance:** Polly v8 resilience pipelines wrap both the session processors and inter-holon evaluations with circuit breakers and exponential backoff, preventing transient network drops or ML inference spikes from corrupting session state.

---

## Graph-Validated ABAC Pipeline

To prevent policy engine misfires from malformed or missing attributes, the authorization pipeline enforces a two-phase gate: **Structural Validation (SHACL)** followed by **Policy Evaluation (Cedar/OPA)**.

### 1. Structural Validation (SHACL)

Before authorization logic executes, the runtime constructs an in-memory RDF graph of the principal, resource, and environment, validating it against a SHACL ontology:

```turtle
@prefix sh: [http://www.w3.org/ns/shacl#](http://www.w3.org/ns/shacl#) .
@prefix xsd: [http://www.w3.org/2001/XMLSchema#](http://www.w3.org/2001/XMLSchema#) .
@prefix res: [https://singularity.research/ontology/resonance#](https://singularity.research/ontology/resonance#) .

res:SecurityPrincipalShape
    a sh:NodeShape ;
    sh:targetClass res:Principal ;
    sh:property [
        sh:path res:clearanceLevel ;
        sh:datatype xsd:integer ;
        sh:minCount 1 ;
        sh:maxCount 1 ;
    ] ;
    sh:property [
        sh:path res:department ;
        sh:in ( "JointProgramOffice" "AdvancedProjects" "DevSecOps" ) ;
        sh:minCount 1 ;
    ] .

res:TelemetryResourceShape
    a sh:NodeShape ;
    sh:targetClass res:TelemetryStream ;
    sh:property [
        sh:path res:classificationLevel ;
        sh:datatype xsd:integer ;
        sh:minCount 1 ;
    ] .
```

### 2. Policy Evaluation (Cedar)

Once the RDF triples pass SHACL validation, the validated graph is serialized into the Cedar policy engine context:

```cedar
permit (
    principal,
    action == Action::"ViewResonanceTelemetry",
    resource
)
when {
    principal.clearanceLevel >= resource.classificationLevel &&
    principal.department == "JointProgramOffice"
};

permit (
    principal,
    action in [Action::"UpdateStrategy", Action::"DeployHolon"],
    resource
)
when {
    principal.department == "DevSecOps" &&
    principal.isPrivilegedSession == true
};
```

---

## Human-in-the-Loop (HITL) Workflows

When resonance anomalies require manual operator intervention or classification, the system transitions into a stateful **Human-in-the-Loop (HITL)** workflow:

* **Cumulative Step Data Model:** Each workflow step captures an immutable ledger of prior step inputs and outputs. Step payload contracts are enforced dynamically in the Blazor UI using `Newtonsoft.Json` and `Newtonsoft.Json.Schema` validation components.
* **ML Pipeline Feedback Loop:** Because the cumulative step models live inside the decoupled Domain layer, completed HITL chains are exported directly into downstream **ML.NET 4.0+** training pipelines to continuously refine automated anomaly detection strategies.
