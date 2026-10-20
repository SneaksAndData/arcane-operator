Fixes/Implements #<issue number>.

### Scope

Implemented:
- Awesome new feature
- And another awesome new feature

Additional changes:
- Refactored `AwesomeClass`
- Removed deprecated `AnotherClass` and `get_awesomeness` from `AwesomeClass`

### Checklist

- [ ] GitHub issue exists for this change.
- [ ] Unit tests added and they pass.
- [ ] Line Coverage is at least 80%.
- [ ] Review requested on `latest` commit.
--- 
- [ ] Documentation updated (if applicable).
 
> [!NOTE]
> Please add/change the user documentation in the [usage.md](../docs/usage.md) and/or [user_scenarios.md](../docs/user_scenarios.md) files.
---
### External dependencies
- [ ] Pull request contains changes in the custom resource definition models.
- [ ] Pull request adds the new stream definition layout version.
 
### External dependencies checklist
- [ ] [arcane-crd](https://github.com/SneaksAndData/arcane-crd) helm chart is updated accordingly and released.
- [ ] [kubectl-plugin-arcane](https://github.com/SneaksAndData/kubectl-plugin-arcane) is updated with the new models.
- [ ] [streaming plugins](https://github.com/SneaksAndData?q=arcane-stream-&type=all&language=&sort=) will be updated accordingly and released.
- [ ] [arcane-stream-mock](https://github.com/SneaksAndData/arcane-stream-mock) is updated and integration tests are passing.
---
- [ ] This commit should be released in the next minor release.
- [ ] This commit should be released in the next patch release.
- [ ] This commit should NOT be released (e.g. documentation changes).