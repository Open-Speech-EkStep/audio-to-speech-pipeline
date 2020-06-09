// AudioFile.h: interface for the CAudioFile class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_AUDIOFILE_H__8E83E060_27AB_4644_96C5_51F6C3ED0DBE__INCLUDED_)
#define AFX_AUDIOFILE_H__8E83E060_27AB_4644_96C5_51F6C3ED0DBE__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#include "s3types.h"
#include "Config.h"

const int MAX_SPEECH_LEN = 100000;	// 12.5 seconds at 16 kHz ??


enum ENDIAN {LITTLE_ENDIAN_, BIG_ENDIAN_};

enum FILE_FORMAT  
						{NIST_FORMAT,
						RAW_FORMAT,
						MSWAV_FORMAT};

class CAudioFile  
{


protected:
	static int16   m_asAudioData[MAX_SPEECH_LEN];
public:
	static float64 m_adAudioData[MAX_SPEECH_LEN];
protected:

	char*   m_pszInWaveFileName;
	char*   m_pszInputEndian;
	char*   m_pszLogFileName;

	FILE*   m_pInWaveFile;
	FILE*   m_pLogFile;

	
	char*   m_pszInputFormat;

	ENDIAN      m_enumInEndian;
	FILE_FORMAT m_enumFileFormat;
//	CConfig*    m_pConfig;
						  

public:
	int32 m_iBlockSize;
	int32 m_iNumBlocks;
	int32 m_iTotalNumSamples;
	int32 m_iNumRead;
	int32 m_iAccNumRead;
	int32 m_iBlockIndex;
public:
	CAudioFile();

	int32 Init(char* pszFileName,
			FILE_FORMAT enumFileformat,
			   ENDIAN endian);

	int32 ReadFile(void); // Read block-by-block
	int32 GetFileSize(void);
	void  RemoveOffset(void);

	virtual ~CAudioFile();

};

#endif // !defined(AFX_AUDIOFILE_H__8E83E060_27AB_4644_96C5_51F6C3ED0DBE__INCLUDED_)
