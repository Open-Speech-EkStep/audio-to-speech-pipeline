// Table.cpp: implementation of the CTable class.
//
//////////////////////////////////////////////////////////////////////

#include "Table.h"

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
///////////////////////////////////////0///////////////////////////////

double  CTable::m_aadTable[MAX_NUM_ROW][MAX_NUM_COLUMN];
CTable::CTable()
{

}

CTable::~CTable()
{

}

int32 CTable::Init(char*pszTableFileName)
{
	m_pszTableFileName = pszTableFileName;

	return (0);
}

int32 CTable::ReadTable()
{
	FILE*   pFile = NULL;
	static char    acLineBuffer[255];
	float64 dAlpha;
	float64 dSNRLevel;
	int32   iRow;
	//int32   i;
	

	pFile = fopen(m_pszTableFileName, "rt");

	if (pFile == NULL)
	{	
		E_FATAL("File cannot be opened.\n");
		
	}

	//fscanf(pFile, "%s", acLineBuffer);

	//printf("%s", acLineBuffer);

	iRow = 0;

	while (fscanf(pFile, "%lf %*s %*s %lf", &dSNRLevel, &dAlpha) != EOF)
	{
	
		m_aadTable[iRow][0] = dSNRLevel;
		m_aadTable[iRow][1] = dAlpha;

		iRow++;

		if (iRow >= MAX_NUM_ROW)
		{
			E_FATAL("Table size overflow!\n");
		}
	}

	m_iNumRow    = iRow;
	m_iNumColumn = 2;

	/*for (i = 0; i < m_iNumRow ; i++)
	{
		printf("%f %f\n", m_aadTable[i][0], m_aadTable[i][1]);
	}*/

	fclose (pFile);

	return (0);
}

float64 CTable::SearchTable(float64 fData)
{

	int32 i;

	if (m_iNumRow <= 0)
	{
		E_FATAL("No table can be found!\n");
	}

	
	for (i = 0; i < m_iNumRow; i++)
	{
		if (fData < m_aadTable[i][1])
		{
			return (m_aadTable[i][0]);
		}
	}

	// Maximum value
	if (i == m_iNumRow)
	{
		E_WARN("Cannot find an appropriate value in the table: Maximum value is used instead.\n");
		return (m_aadTable[m_iNumRow - 1][0]);
	}

	// Minimum value
	E_WARN("Cannot find an appropriate value in the table: Minimum value is used instead.\n");
	return (m_aadTable[0][0]);
}

