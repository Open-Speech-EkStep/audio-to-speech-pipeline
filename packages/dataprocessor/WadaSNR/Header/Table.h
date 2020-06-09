// Table.h: interface for the CTable class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_TABLE_H__62E746F2_9C8A_43A4_920B_1ADC222F4F65__INCLUDED_)
#define AFX_TABLE_H__62E746F2_9C8A_43A4_920B_1ADC222F4F65__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#include "s3types.h"

const int MAX_NUM_ROW    = 500;
const int MAX_NUM_COLUMN = 2;



class CTable  
{

public:
	static double m_aadTable[MAX_NUM_ROW][MAX_NUM_COLUMN];
	int m_iNumRow;
	int m_iNumColumn;

	char* m_pszTableFileName;
	
	CTable();
	virtual ~CTable();

	int32 Init(char* pszTableFileName);
	int32 ReadTable();
	double SearchTable(double dVal);

};

#endif // !defined(AFX_TABLE_H__62E746F2_9C8A_43A4_920B_1ADC222F4F65__INCLUDED_)
