$performanceScripts = @(
    @{"Name" = "Sequential GetContainer"; "script" = {echo "Sequential"; Start-Sleep -Seconds 1;}},
    @{"Name" = "Master/Slave GetContainer"; "script" = {echo "Master/Slave";  Start-Sleep -Seconds 5;}},
    @{"Name" = "Begin/End GetContainer"; "script" = {echo "Begin/End";  Start-Sleep -Seconds 10;}}
)


foreach($performanceTest in $performanceScripts)
{
    echo $performanceTest["Name"];
    $times = Measure-Command $performanceTest["script"];
    $timeInMs = $times.TotalMilliseconds;
    echo "$timeInMs ms"
}