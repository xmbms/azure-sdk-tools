$benchmarkContext = New-AzureStorageContext -StorageAccountName $env:STORAGE_BENCHMARK_ACCOUNT -StorageAccountKey $ENV:STORAGE_BENCHMARK_KEY
$performanceScripts = @(
    @{"Name" = "Sequential GetContainer"; "script" = {Get-AzureStorageContainer -Context $benchmarkContext }},
    @{"Name" = "Master/Slave GetContainer"; "script" = {Get-AzureStorageContainerInMasterSlave -Context $benchmarkContext}}
)


foreach($performanceTest in $performanceScripts)
{
    echo $performanceTest["Name"];
    $times = Measure-Command $performanceTest["script"];
    $timeInMs = $times.TotalMilliseconds;
    echo "$timeInMs ms"
}