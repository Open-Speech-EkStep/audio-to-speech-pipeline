#!/usr/bin/perl

open (inFile, $ARGV[0]);

$iCount = 0;

while ($szLine = <inFile>)
{
	chomp ($szLine);

	@array = split('\s+', $szLine);

	#print (@array);

	for ($i = 0; $i < @array - 1; $i++)
	{
	#	print ("$i : $array[$i]\n");


		$data[$i] += $array[$i+1];

	}


	$iCount++;

#	die ("Running up to here.\n");

}

for ($i = 0; $i < @array - 1; $i++)
{
	$dSNR   = -20 + $i;
	$dAlpha = $data[$i] /  $iCount;

	print ("$dSNR dB : $dAlpha\n");
}	

close (inFile);
