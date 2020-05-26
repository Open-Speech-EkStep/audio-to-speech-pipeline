// SNREst.cpp: implementation of the CSNREst class.
//
//////////////////////////////////////////////////////////////////////

#include <string.h>
#include "SNREst.h"

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

extern FILE* g_logFile;

CSNREst::CSNREst()
{
	m_dAccSigEng   = 0.0;
	m_dAccNoiseEng = 0.0;

}

CSNREst::~CSNREst()
{


}

int32 CSNREst::Init(char* pszTableFileName, CConfig* pConfig)
{

	//audioFile.Init(pszFileName);
	m_tableVal.Init(pszTableFileName);

	m_pConfig = pConfig;

	
	m_tableVal.ReadTable();

	return (0);
}

float64 CSNREst::Run(char* pszAudioFileName)
{
	int32 iReturn;

	if (strcmp(m_pConfig->m_pszInputFormat, "nist") == 0)
	{
		m_audioFile.Init(pszAudioFileName, NIST_FORMAT, LITTLE_ENDIAN_);
	}
	else if (strcmp(m_pConfig->m_pszInputFormat, "mswav") == 0)
	{
		m_audioFile.Init(pszAudioFileName, MSWAV_FORMAT, LITTLE_ENDIAN_);
	}
	else if (strcmp(m_pConfig->m_pszInputFormat, "raw") == 0)
	{
		m_audioFile.Init(pszAudioFileName, RAW_FORMAT, LITTLE_ENDIAN_);
	}
	else
	{
		E_FATAL("unsupported input file type.!");
	}

	iReturn = 1;

	m_dAccSigEng   = 0.0;
	m_dAccNoiseEng = 0.0;

	while (iReturn)
	{
		iReturn = m_audioFile.ReadFile();

		m_audioFile.RemoveOffset();

		ComputeSNR();
	}

	printf("\n=====================================================================\n");
	printf("Total SNR is %lf dB.\n", 10 * log10(m_dAccSigEng / m_dAccNoiseEng));
	printf("=====================================================================\n");

	//

	// Compare the precomputed table

	return (0);
}

void CSNREst::ComputeSNR()
{

	int32     iAudioFileLen = m_audioFile.m_iNumRead;
	float64*  pdAudioData   = m_audioFile.m_adAudioData;
	float64   dVal1, dVal2, dVal3;
	float64   dSNR;
	float64   dEng, dSigEng, dNoiseEng;
	float64   dFactor;

	float64 dData;
	int32 i;
	

	if (iAudioFileLen <= 0)
	{
		E_FATAL("Zero audio file length!");
	}

	if (iAudioFileLen < 8000)
	{
		E_WARN("Insufficient audio file length in this block!");

		return;
	}


	dVal1 = 0.0;
	dVal2 = 0.0;
	
	dEng = 0.0;

	for (i = 0; i < iAudioFileLen; i++)
	{
		if (ABS(pdAudioData[i]) < 1e-10)
			dData = 1e-10;
		else
			dData = ABS(pdAudioData[i]);

		dVal1 += dData;

		dEng += (pdAudioData[i] * pdAudioData[i]);

		dVal2 += log(dData);

		
	}

	if (dVal1 == 0)
	{
		dVal1 = 1e-10;
	}

	dVal1 /= iAudioFileLen;
	dVal2 /= iAudioFileLen;
	dVal3 = log(dVal1) - dVal2;

	if (g_logFile)
	{

	fprintf (g_logFile, "E[|z|] = %f E[log|z|] =  %f\n", dVal1, dVal2);



	fprintf(g_logFile,"log(E[|z|]) - E[log(|z|)] = %f\n", dVal3);
	}

	//this->m_tableVal
	dSNR = m_tableVal.SearchTable(dVal3);

	dFactor = pow(10, dSNR / 10);

	dNoiseEng   = dEng / (1 + dFactor);

	dSigEng = dEng * dFactor / (1 + dFactor);

	m_dAccSigEng   += dSigEng;
	m_dAccNoiseEng += dNoiseEng;

	if (g_logFile)
	{

	fprintf(g_logFile, "The computed SNR value is %lf.\n", dSNR);

	fprintf(g_logFile, "Signal energy in this block : %lf.\n", dSigEng);

	fprintf(g_logFile, "Noise energy in this block : %lf.\n", dNoiseEng);

	fprintf(g_logFile, "Computed SNR : %lf.\n", 10 * log10(dSigEng / dNoiseEng));
	}

}

