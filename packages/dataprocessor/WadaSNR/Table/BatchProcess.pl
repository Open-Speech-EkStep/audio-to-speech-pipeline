#!/usr/bin/perl

@aszFileList = `find . -iname 'Mat*txt'`; 

#print (@aszFileList);

foreach $szFile (@aszFileList)
{
	chomp($szFile);
	$szFile =~ /^.*_(.*)\.txt.*$/;

	#print ("$1\n");

	$szNewFile = "Alpha${1}.txt";

	system ("rm -fr $szNewFile");

	$szCommand = "Process.pl $szFile > $szNewFile";

	print ("$szCommand\n");

	system ("$szCommand");
}

