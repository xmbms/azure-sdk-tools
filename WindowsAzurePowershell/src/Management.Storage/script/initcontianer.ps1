$benchmarkContext = New-AzureStorageContext -StorageAccountName $env:STORAGE_BENCHMARK_ACCOUNT -StorageAccountKey $ENV:STORAGE_BENCHMARK_KEY

$count = 100;

for($i= 0; $i -lt $count; $i++)
{
    $str = [System.String]::Format("container{0}", $i.ToString("D5"));
    
    $permission = "off";

    switch($i % 3)
    {
        0 {$permission = "off";}
        1 {$permission = "blob";}
        2 {$permission = "container";}
    }


    New-AzureStorageContainer $str -Permission $permission -Context $benchmarkContext
}