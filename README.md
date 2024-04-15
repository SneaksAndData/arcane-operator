## Arcane Operator


# SnD.SDK ![NuGet Version](https://img.shields.io/nuget/v/SnD.Sdk)

Core SDK for Sneaks & Data OSS Projects written in C#. Use cases include:
- Easy injection of commonly used services like blob, queue etc.
- Injection of Kubernetes client(s). We support both simple in-cluster mode and multi-cluster mode, allowing you to write applications targeting arbitrary clusters
- Wrappers around Kubernetes library that attach Polly retry policies. This is implemented where needed - open a PR if your method is not covered yet
- Service configurators for Scylla/Cassandra/AstraDB

## Functional Extensions

This SDK provides several methods that allow you to write more functional-style async code. We specifically target most annoying use cases:
- `await await ...`
- `try { await ... return await ... } catch { await ...}`

and more around chaining awaits. Check `Map`, `TryMap`, `FlatMap` for details.

## Contributing

This project uses a few simple guidelines:

- Add unit tests for every function you've added, excluding wrappers around vendor code. Add `ExcludeFromCodeCoverage` for those
- Run `dotnet format` from project directory before pushing a commit
- Write functional-style code using the Snd.Sdk project: https://github.com/SneaksAndData/esd-services-sdk/
- Write `Theory`, not `Fact` tests when possible - test edge cases!
- Aim for 100% code coverage of your commit, 80% code coverage in the final report
