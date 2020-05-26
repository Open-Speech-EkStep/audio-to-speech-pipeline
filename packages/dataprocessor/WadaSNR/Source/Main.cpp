#include <stdio.h>

/////////////////////////////////////////////////////////////////////////////
//
//	File Name : MainSynap2ANFeat.cpp
//
//													Feb. 23, 2006
//
//	Programmed by Chanwoo Kim
//
//  Comments:
//
//
//
//	Created						Feb. 23, 2006, Chanwoo Kim
//	Including the anmodel program			Mar. 13, 2006, Chanwoo Kim
//	Modifying the equalization algorithm		Mar. 13, 2006, Chanwoo Kim
//	Option for windoe type selection                July 11, 2006, Chanwoo Kim 	
//  	Option for variance normalization         	July 11, 2006, Chanwoo Kim
//  	Option for MS wave		         	July 11, 2006, Chanwoo Kim
//

#include <stdio.h>
#include <stdlib.h>

// SPHINX files
#include "libutil.h"

#include "s3types.h"
#include "fe.h"

FILE* g_logFile = NULL;

/////////////////////////////////////////////////////////////////////////////
// Showing example
const char szExampleStr[] =
 "Example: \n\
This example creates a cepstral file named \
\"output.mfc\" from an input audio file named \"input.raw\", \
which is a raw  \
audio file (no header information), which was originally sampled at 16kHz. \n \
\n									\
SNREst 	        -ifmt raw		\n	\
				-i input.raw    \n	\
				-ofmt	afloat	\n	\
				-o   output.txt \n";

static arg_t defn[] = 
{
	{"-i",
	ARG_STRING,
	NULL,
	"Input file name"},
	{"-t",
	ARG_STRING,
	NULL,
	"Table file name"},
	{"-srate",
	ARG_FLOAT64,
	"16000.0",
	"The sampling rate of the synapse signal"
	},
/*	{ "-blocksize",
    ARG_INT32,
    DEFAULT_BLOCKSIZE,			
    "Block size, used to limit the number of samples used at a time when reading very large audio files" },*/
	{"-input_endian",
	ARG_STRING,
	"little",
	"input endianism"},
	{"-ifmt",
	ARG_STRING,
	"nist",
	"Input file format (raw, nist, mswav)"}, // MS wave does not prase header. It only skips the first 44 bytes.
	{ "-logfn",
    ARG_STRING,
    NULL,
    "Log file (default stdout/stderr)" },

	{ NULL, ARG_INT32,  NULL, NULL }
};


/////////////////////////////////////////////////////////////////////////////
//
// Function:
//
// Parameter:
//
// Return value:
//
// Comment:
//
// History
//
//
//	Programmed by Chanwoo Kim 	(Mar 28, 2007)


#include "Config.h"
#include "SNREst.h"

int32 main(int32 argc, char** argv)
{
	// Configuration class
	CConfig			config;
	CSNREst         snrEst;
	float64         dSNR;

	print_appl_info(argv[0]);

	cmd_ln_appl_enter(argc, argv, "default.arg", defn);

	

	/////////////////////////////////////////////////////////////////////////
	//
	//	Command line parsing and setting the configuration information

	/////////////////////////////////////////////////////////////////////////
	//
	// Input and output file
	config.m_pszInWaveFileName	= cmd_ln_str("-i");
	config.m_pszInputEndian     = cmd_ln_str("-input_endian");
	config.m_pszInputFormat     = cmd_ln_str("-ifmt");
	config.m_dSampRate			= cmd_ln_float64("-srate");
	config.m_pszTableFileName   = cmd_ln_str("-t");
	config.m_pszLogFileName     = cmd_ln_str("-logfn");

	config.Init();

	
	snrEst.Init(config.m_pszTableFileName, &config);
	dSNR = snrEst.Run(config.m_pszInWaveFileName);

	/////////////////////////////////////////////////////////////////////////
	//
	// AN model processing
	
	
	cmd_ln_free();
	cmd_ln_appl_exit();


	return (0);
}



