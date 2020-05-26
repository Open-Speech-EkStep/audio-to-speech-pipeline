// Config.h: interface for the CConfig class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_CONFIG_H__26D8DE17_4178_4CBA_8524_D0109B2B6301__INCLUDED_)
#define AFX_CONFIG_H__26D8DE17_4178_4CBA_8524_D0109B2B6301__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#include "s3types.h"

class CConfig  
{
public:
	CConfig();
	virtual ~CConfig();

public:

	char*   m_pszInWaveFileName;
	char*   m_pszLogFileName;
	char*   m_pszTableFileName;
	char*   m_pszInputEndian;
	char*   m_pszInputFormat;
	
	
	int32 m_iTotalSpeechLen;
	int32 m_iBlockSize;
	int32 m_iNumBlocks;
	int32 m_iSpeechLen;

	int32 Init(void);

	float64 m_dSampRate;

};

#endif // !defined(AFX_CONFIG_H__26D8DE17_4178_4CBA_8524_D0109B2B6301__INCLUDED_)
