Import-Module "..\..\..\..\Package\Debug\Microsoft.WindowsAzure.Management.Storage.dll"

$benchmarkContext = New-AzureStorageContext -StorageAccountName $env:STORAGE_BENCHMARK_ACCOUNT -StorageAccountKey $ENV:STORAGE_BENCHMARK_KEY -Protocol Http

$performanceScripts = @(
    @{"Name" = "Sequential GetContainer"; "script" = {Get-AzureStorageContainer -Context $benchmarkContext }},
    @{"Name" = "Async GetContainer"; "script" = {Get-AsyncContainer -Context $benchmarkContext}}
)

for($thread = 5; $thread -lt 100; $thread += 5)
{
    $name =  "Master/Slave GetContainer using $thread threads";
    $script = {Get-MsContainer -Context $benchmarkContext -Thread $thread}
    $cmd = @{"Name" = $name; "script" = $script}
    $performanceScripts += $cmd;
}


foreach($performanceTest in $performanceScripts)
{
    echo $performanceTest["Name"];
    $times = Measure-Command $performanceTest["script"];
    $timeInMs = $times.TotalMilliseconds;
    echo "$timeInMs ms"
}