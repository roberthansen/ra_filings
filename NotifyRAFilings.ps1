# Runs the ra_filing script to send a summary of the current compliance check
# to the RAFilings@cpuc.ca.gov inbox via Kiteworks.

$ExistingDrive = Get-PSDrive -Name Z
If(-Not $ExistingDrive) {
    New-PSDrive -Name Z -PSProvider FileSystem -Root \\Sf150pyclfs26\PYCLIENTFS -Persist
}
& Z:
& C:\Miniconda3\shell\condabin\conda-hook.ps1
& conda activate Z:\Users\svc_energyRA\svc_conda
& Set-Location Z:\Users\svc_energyRA\ra_filings
& anaconda-project run notify
& conda deactivate
& C:
If (-Not $ExistingDrive) {
    Remove-PSDrive Z
}