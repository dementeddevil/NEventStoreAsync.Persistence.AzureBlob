$configurationdefault = "Release"
$artifacts = "../../artifacts"
$nugetartifacts = "artifacts"

$configuration = Read-Host 'Configuration to build [default: Release] ?'
if ($configuration -eq '') {
    $configuration = $configurationdefault
}
$runtests = Read-Host 'Run Tests (y / n) [default:n] ?'

$patchdependencies = Read-Host 'Patch version of dependencies [default:n] ?'

# Consider using NuGet to download the package (GitVersion.CommandLine)
choco install gitversion.portable --pre --y
choco upgrade gitversion.portable --pre --y
choco install nuget.commandline
choco upgrade nuget.commandline

# Display minimal restore information
dotnet restore ./src/NEventStoreAsync.Persistence.AzureBlob.Core.sln --verbosity m

# GitVersion (for the main module)
Write-Host "Running GitVersion for the Project"
$str = gitversion /updateAssemblyInfo ./src/VersionAssemblyInfo.cs | out-string
$json = convertFrom-json $str
$nugetversion = $json.NuGetVersion

# Now we need to patch the AssemblyInfo for submodules
if ($patchdependencies -eq "y") {
	Write-Host "Running GitVersion for the Dependencies"
	gitversion ".\dependencies\NEventStore" /updateAssemblyInfo | Out-Null
}

# Build
Write-Host "Building: "$nugetversion" "$configuration
dotnet build ./src/NEventStoreAsync.Persistence.AzureBlob.Core.sln -c $configuration --no-restore

# Testing
if ($runtests -eq "y") {
    Write-Host "Executing Tests"
    dotnet test ./src/NEventStoreAsync.Persistence.AzureBlob.Core.sln -c $configuration --no-build
    Write-Host "Tests Execution Complated"
}

# NuGet packages
Write-Host "NuGet Packages creation"
# not working well, without a nuspec file ProjectReferences get the wrong version number
#dotnet pack ./src/NEventStore.Persistence.Sql/NEventStore.Persistence.Sql.Core.csproj -c $configuration --no-build -o $artifacts -p:PackageVersion=$nugetversion

# not working well, with a reference to a nuspec file, it seems i'm not able to pass in the $configuration to retrieve the correct files
#Write-Host "dotnet pack ./src/NEventStore.Persistence.Sql/NEventStore.Persistence.Sql.Core.csproj --no-build -c $configuration -o $artifacts -p:NuspecProperties=""pippo=$configuration;version=$nugetversion"""
#dotnet pack ./src/NEventStore.Persistence.Sql/NEventStore.Persistence.Sql.Core.csproj --no-build -c $configuration -o $artifacts -p:NuspecFile="" -p:NuspecProperties="pippo=$configuration;version=$nugetversion"

#Write-Host nuget pack ./src/.nuget/NEventStore.Persistence.Sql.nuspec -properties "version=$nugetversion;configuration=$configuration"
nuget pack ./src/.nuget/NEventStoreAsync.Persistence.AzureBlob.nuspec -properties "version=$nugetversion;configuration=$configuration" -OutputDirectory $nugetartifacts