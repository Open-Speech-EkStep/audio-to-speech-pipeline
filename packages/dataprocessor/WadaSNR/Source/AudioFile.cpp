// AudioFile.cpp: implementation of the CAudioFile class.
//
//////////////////////////////////////////////////////////////////////

#include "AudioFile.h"
#include <stdio.h>

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

short  CAudioFile::m_asAudioData[MAX_SPEECH_LEN];
double CAudioFile::m_adAudioData[MAX_SPEECH_LEN];

extern FILE* g_logFile;

CAudioFile::CAudioFile()
{
	m_pInWaveFile = NULL;
	m_pszLogFileName = NULL;
	m_iNumRead    = 0;
	m_iAccNumRead = 0;
	m_iBlockIndex = 0;
}

CAudioFile::~CAudioFile()
{
	if (m_pInWaveFile != NULL)
		fclose (m_pInWaveFile);

}

int32 CAudioFile::Init(char* pszFileName, 
			FILE_FORMAT enumFileformat,
				
					  ENDIAN endian)
{
	m_pszInWaveFileName = pszFileName;
	
	m_enumInEndian      = endian;

	m_enumFileFormat  = enumFileformat;

	m_pInWaveFile = fopen(m_pszInWaveFileName, "rb");

	switch (enumFileformat)
	{
	case MSWAV_FORMAT:
		fseek(m_pInWaveFile, 44, SEEK_SET);
		break;
	case NIST_FORMAT:
		fseek(m_pInWaveFile, 1024, SEEK_SET);
		break;

	default:
		break;
	}

	if (m_pInWaveFile == NULL)
	{
		E_FATAL("File cannot be opened.\n");

		exit (0);
	}

	GetFileSize();

	return (0);
}


int32 CAudioFile::ReadFile()
{
	int32 iNumRead;
	int32 i;

	if (g_logFile)
	{

	fprintf(g_logFile, "================================================================================\n");
	fprintf(g_logFile, " Block %d is now being read.\n", m_iBlockIndex);
	fprintf(g_logFile, " Max block size : %d\n", MAX_SPEECH_LEN);

	fprintf(g_logFile, " Total number of samples : %d\n", m_iTotalNumSamples);
	}


	memset(m_asAudioData, 0, sizeof(short) * MAX_SPEECH_LEN);
	iNumRead =fread(m_asAudioData, 2, MAX_SPEECH_LEN, m_pInWaveFile);

	m_iNumRead     = iNumRead;
	m_iAccNumRead += m_iNumRead;

	for (i = 0; i < iNumRead; i++)
	{
		m_adAudioData[i] = m_asAudioData[i];
	}

		if (g_logFile)
	{
	fprintf(g_logFile, "%d has been read in this block.\n", m_iNumRead);
	fprintf(g_logFile, " Samples read up to now : %d\n", m_iAccNumRead);
		}



	if (iNumRead < MAX_SPEECH_LEN)
	{
		if (g_logFile)
	{
		fprintf(g_logFile, "File read has been finished.\n");
		}
	}

			if (g_logFile)
	{
	fprintf(g_logFile, "%d are left in the audio file.\n", m_iTotalNumSamples - m_iAccNumRead);

	fprintf(g_logFile, "================================================================================\n");
			}
		
	m_iBlockIndex++;

	if (iNumRead >= MAX_SPEECH_LEN )
		return (1); // Not finished
	else
		return (0); // Finished reading
}

int32 CAudioFile::GetFileSize()
{
	int32 iResult;
#ifdef WIN32
	struct _stat fileStat;
#else
	struct stat  fileStat;
#endif

	
#ifdef WIN32
	iResult = _stat(m_pszInWaveFileName, &fileStat);
#else
	iResult = stat(m_pszInWaveFileName, &fileStat);
#endif

	switch (m_enumFileFormat)
	{
	case RAW_FORMAT:
	m_iTotalNumSamples = fileStat.st_size / 2;
		break;
	case MSWAV_FORMAT:
	m_iTotalNumSamples = (fileStat.st_size  - 44)/ 2;
		break;
	case NIST_FORMAT:
	m_iTotalNumSamples = (fileStat.st_size  - 1024)/ 2;
		break;
	default:
		E_FATAL("Unsupported file format!!!");
		break;
	}

			if (g_logFile)
	{
	fprintf(g_logFile,"=======================================================================================\n");
	fprintf(g_logFile, "[CAudioFile] The entire file size of %s is %d samples.\n", m_pszInWaveFileName, fileStat.st_size / 2);
	fprintf(g_logFile,"=======================================================================================\n");
			}

	return (0);
}

void CAudioFile::RemoveOffset(void)
{
	int32   i;
	float64 dMean;

	if (m_iNumRead == 0)
	{
		E_FATAL("[CAudioFile] No data samples in the buffer.\n");
	}

	dMean = 0.0;

	for (i = 0; i < m_iNumRead; i++)
	{
		dMean += m_adAudioData[i]; 
	}

	dMean /= m_iNumRead;

			if (g_logFile)
			{
	

	fprintf(g_logFile, "[CAudioFile] The offest value is %f.\n", dMean);
			}

	for (i = 0; i < m_iNumRead; i++)
	{
		m_adAudioData[i] -= dMean;
	}
}
