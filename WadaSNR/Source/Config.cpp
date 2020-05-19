// Config.cpp: implementation of the CConfig class.
//
//////////////////////////////////////////////////////////////////////

#include "Config.h"
#include "SNREst.h"

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

extern FILE* g_logFile;

CConfig::CConfig()
{
	m_pszLogFileName    = NULL;
	m_pszInWaveFileName = NULL;
	g_logFile = NULL;
}

CConfig::~CConfig()
{
	if (g_logFile != NULL)
	{
		fclose (g_logFile);
	}

}


int32 CConfig::Init(void)
{
#ifdef WIN32
	struct _stat fileStat;
#else
	struct stat  fileStat;
#endif
	int32        iResult;
	int32        iNumRetry;

	if (m_pszInWaveFileName == NULL)
	{
		fprintf(stderr, "The input file name should be specified.!\n");

		exit (0);
	}

#ifdef WIN32
	iResult = _stat(m_pszInWaveFileName, &fileStat);
#else
	iResult = stat(m_pszInWaveFileName, &fileStat);
#endif



	if (strcmp(this->m_pszInputFormat, "nist") == 0)
		this->m_iTotalSpeechLen = (fileStat.st_size - 1024) / sizeof(short);
	else
		this->m_iTotalSpeechLen = (fileStat.st_size) / sizeof(short);

	m_iBlockSize = MAX_SPEECH_LEN;

	m_iNumBlocks = m_iSpeechLen / m_iBlockSize;

	if (m_pszLogFileName != NULL)
	{
		g_logFile = fopen(m_pszLogFileName, "wt");

		if (g_logFile == NULL)
		{
			E_FATAL("Log file open error!\n");
		}
	}
	else
	{
		E_INFO("Logfile will not be used.");
	}

	return (0);
}

