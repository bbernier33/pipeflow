# Windows Race Testing

Pipeflow's concurrency changes must be validated with Go's race detector.
Windows race builds require CGO and a compatible MinGW-w64 C runtime.

The validated local toolchain is MSYS2 UCRT64 GCC. Confirm compatibility using
Go's required library check:

```powershell
& 'C:\msys64\ucrt64\bin\gcc.exe' --print-file-name libsynchronization.a
```

The command must print a full path rather than only `libsynchronization.a`.

Run the suite from PowerShell with command-scoped environment settings:

```powershell
$env:PATH = 'C:\msys64\ucrt64\bin;' + $env:PATH
$env:CC = 'gcc'
$env:CGO_ENABLED = '1'
go test -race ./...
```

Keeping these settings command-scoped avoids making ordinary pure-Go builds
depend globally on a local compiler installation.

After race fixes, rerun stability and static checks:

```powershell
go test -count=100 ./...
go vet ./...
```

On restricted environments, cgo may need permission to create and execute
temporary compiler and linker artifacts outside the repository sandbox.
