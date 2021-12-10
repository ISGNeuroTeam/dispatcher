When preparing a release, you should check the following points:
1. in build.sbt the version is changed to next
```
...
version: = "x.y.z"
...
```

2. in application.conf, the version in the application name is changed to match
with the version from point 1
```
spark {
  ...
  appName = "Dispatcher (x.y.z)"
  ...
}
```

3. CHANGELOG.md has been updated in accordance with the changes in the release.
4. RELEASENOTES.md has been updated in accordance with the changes in the release.