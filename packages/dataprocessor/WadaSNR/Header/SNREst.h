// SNREst.h: interface for the CSNREst class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_SNREST_H__80CDF79A_2627_4470_9274_FFD110D94F90__INCLUDED_)
#define AFX_SNREST_H__80CDF79A_2627_4470_9274_FFD110D94F90__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#include "AudioFile.h"
#include "Table.h"
#include "Config.h"

#define ABS(x) (((x) > 0)? (x) : -(x))

class CSNREst  
{
public:
	CAudioFile m_audioFile;
	CTable     m_tableVal;
	CConfig*   m_pConfig;

	float64    m_dSigEngBlock;

	float64    m_dAccSigEng;
	float64    m_dAccNoiseEng;

	float64    m_dNoiseEngBlock;
	
	float64    m_dSNRBlock;
	float64    m_dSNRAcc;
	
	
	CSNREst();
	virtual ~CSNREst();

	int32   Init(char* pszFileName,
			CConfig* pConfig);
	float64 Run(char* pszAudioFileName);

	void ComputeSNR(void);

};

#endif // !defined(AFX_SNREST_H__80CDF79A_2627_4470_9274_FFD110D94F90__INCLUDED_)
