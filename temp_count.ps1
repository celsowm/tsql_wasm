Get-ChildItem -Recurse -File | ForEach-Object {
  $lines = @(Get-Content $_.FullName -ErrorAction SilentlyContinue).Count
  [PSCustomObject]@{File=$_.FullName; Lines=$lines}
} | Sort-Object Lines -Descending | Select-Object -First 10 | Format-Table -AutoSize
