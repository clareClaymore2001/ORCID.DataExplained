import os,time,random
import xml.etree.ElementTree as ET
import pandas as pd
import glob
import csv

import multiprocessing as mp
import concurrent.futures
from tqdm import tqdm
from joblib import Parallel, delayed

def readCsv(location):
    with open(location,encoding='utf-8-sig') as file:
        csv_file = csv.reader(file)
        next(csv_file)
        return [line for line in csv_file]

def readCsv_perData(location):
    result = []

    with open(location,encoding='utf-8-sig') as file:
        csv_file = csv.reader(file)
        next(csv_file)

        for row in tqdm(csv_file):
            resultRow = []

            for item in row:
                if item != '':
                    itemSplited = item[1:-1].split(', ')
                    resultItem = {}

                    for key in itemSplited:
                        keyFind = key.find(':')
                        resultItem[key[1:keyFind-1]] = key[keyFind+3:-1]

                    resultRow.append(resultItem)

            result.append(resultRow)

    return result
    
def readTsv_ID_RINGGOLD_TO_ISNI(location):
    with open(location,encoding='utf-8-sig') as file:
        tsv_file = csv.reader(file, delimiter = '\t')
        next(tsv_file)
        return [line for line in tsv_file if line[0] != '']

def nWorkers(array):
    max_workers = 2 * mp.cpu_count()
    elementLen = len(array)
    return elementLen if max_workers > elementLen else max_workers

def batch_file(array,n_workers):
  file_len = len(array)
  batch_size = round(file_len / n_workers)
  batches = [array[ix : ix + batch_size]
    for ix in tqdm(range(0,file_len,batch_size))]
  return batches

def flatten(matrix,times):
    if times == 0:
        return matrix
    else:
        return [i for j in flatten(matrix,times-1) for i in j]

def exportFile(array,fileName):
    df = pd.DataFrame(array)
    output_file = os.path.join(os.getcwd(),fileName)
    df.to_csv(output_file,index=False,encoding='utf-8-sig')

def perData_process(xml_file,ns):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    perDataProcess = []

    for actSummary in root.findall('act:activities-summary',ns):
        for edu in actSummary.findall('act:educations',ns):
            for eduAffiliationGroup in edu.findall('act:affiliation-group',ns):
                for eduSummary in eduAffiliationGroup.findall('edu:education-summary',ns):
                    eduStartDate = 0
                    eduDisOrgID = eduDisSource = None

                    for eduStartDateElement in eduSummary.findall('com:start-date',ns):
                        for eduStartDateYearElement in eduStartDateElement.findall('com:year',ns):
                            eduStartDate += 10000 * int(eduStartDateYearElement.text)
                        for eduStartDateMonthElement in eduStartDateElement.findall('com:month',ns):
                            eduStartDate += 100 * int(eduStartDateMonthElement.text)
                        for eduStartDateDayElement in eduStartDateElement.findall('com:day',ns):
                            eduStartDate += int(eduStartDateDayElement.text)

                    for eduOrg in eduSummary.findall('com:organization',ns):
                        for eduDisOrg in eduOrg.findall('com:disambiguated-organization',ns):
                            for eduDisSourceElement in eduDisOrg.findall('com:disambiguation-source',ns):
                                eduDisSource = (eduDisSourceElement.text)
                            for eduDisOrgIDElement in eduDisOrg.findall('com:disambiguated-organization-identifier',ns):
                                eduDisOrgID = (eduDisOrgIDElement.text)

                    if eduStartDate != 0 and eduDisOrgID is not None and eduDisSource is not None:
                        perDataProcess.append({'StartDate': eduStartDate, 'DisSource': eduDisSource, 'DisOrgID': eduDisOrgID})

        for emp in actSummary.findall('act:employments',ns):
            for empAffiliationGroup in emp.findall('act:affiliation-group',ns):
                for empSummary in empAffiliationGroup.findall('emp:employment-summary',ns):
                    empStartDate = 0
                    empDisOrgID = empDisSource = None

                    for empStartDateElement in empSummary.findall('com:start-date',ns):
                        for empStartDateYearElement in empStartDateElement.findall('com:year',ns):
                            empStartDate += 10000 * int(empStartDateYearElement.text)
                        for empStartDateMonthElement in empStartDateElement.findall('com:month',ns):
                            empStartDate += 100 * int(empStartDateMonthElement.text)
                        for empStartDateDayElement in empStartDateElement.findall('com:day',ns):
                            empStartDate += int(empStartDateDayElement.text)

                    for empOrg in empSummary.findall('com:organization',ns):
                        for empDisOrg in empOrg.findall('com:disambiguated-organization',ns):
                            for empDisSourceElement in empDisOrg.findall('com:disambiguation-source',ns):
                                empDisSource = (empDisSourceElement.text)
                            for empDisOrgIDElement in empDisOrg.findall('com:disambiguated-organization-identifier',ns):
                                empDisOrgID = (empDisOrgIDElement.text)

                    if empStartDate != 0 and empDisOrgID is not None and empDisSource is not None:
                        perDataProcess.append({'StartDate': empStartDate, 'DisSource': empDisSource, 'DisOrgID': empDisOrgID})

    return perDataProcess

def perData_proc_batch(batch,ns):
  return [perData_process(xml_file,ns)
    for xml_file in tqdm(batch)]

def perData_pair_FUNDREF(string,string_list):
    for item in string_list:
        if string in item:
            return item
    return -1

def perData_pair_modify(i,array,x,y):
    array[x][y]['DisOrgID'] = i[0]
    array[x][y]['OrgLocationCountry'] = i[18]
    array[x][y]['OrgLocationDetails'] = i[22]
    array[x][y]['OrgName'] = i[26]
    array[x][y]['OrgType'] = i[30]
    return array

def perData_pair_process(i,array,x,y,TR,TR_INDEX_ROR,times):
    if times == 0:
        return perData_pair_modify(i,array,x,y)
    else:
        arrayXY = array[x][y]
        iRelation = i[28]
        iStart = iRelation.find('parent')

        if iStart == -1:
            return perData_pair_modify(i,array,x,y)
        else:
            iEnd = iRelation.find(';',iStart)

            if iEnd == -1:
                iParentStr = iRelation[iStart+8:]
            else:
                iParentStr = iRelation[iStart+8:iEnd]

            if iParentStr.find(',') == -1 and TR_INDEX_ROR.count(iParentStr) > 0:
                iParent = TR[TR_INDEX_ROR.index(iParentStr)]

                if iParent == i:
                    return perData_pair_modify(i,array,x,y)
                else:
                    return perData_pair_process(iParent,array,x,y,TR,TR_INDEX_ROR,times-1)

            else:
                return perData_pair_modify(i,array,x,y)

def perData_pair_MAIN(array,RTI,TR,RTI_INDEX,TR_INDEX_ROR,TR_INDEX_FUNDREF,TR_INDEX_GRID,TR_INDEX_ISNI):
    x = 0
    lenArray = len(array)

    with tqdm(total = lenArray) as pbar:
        while x < lenArray:
            pbar.update(1)
            arrayX = array[x]

            y = 0
            lenArrayX = len(arrayX)
            while y < lenArrayX:
                arrayXY = arrayX[y]
                disSource = arrayXY['DisSource']
                disOrgID = arrayXY['DisOrgID']

                match disSource:
                    case 'ROR':
                        if TR_INDEX_ROR.count(disOrgID) == 0:
                            del array[x][y]
                            lenArrayX -= 1
                        else:
                            array = perData_pair_process(TR[TR_INDEX_ROR.index(disOrgID)],array,x,y,TR,TR_INDEX_ROR,16)
                            y += 1

                    case 'FUNDREF':
                        disOrgID = arrayXY['DisOrgID'][27:]
                        j = perData_pair_FUNDREF(disOrgID,TR_INDEX_FUNDREF)

                        if j == -1:
                            del array[x][y]
                            lenArrayX -= 1
                        else:
                            array = perData_pair_process(TR[TR_INDEX_FUNDREF.index(j)],array,x,y,TR,TR_INDEX_ROR,16)
                            y += 1

                    case 'GRID':
                        if TR_INDEX_GRID.count(disOrgID) == 0:
                            del array[x][y]
                            lenArrayX -= 1
                        else:
                            array = perData_pair_process(TR[TR_INDEX_GRID.index(disOrgID)],array,x,y,TR,TR_INDEX_ROR,16)
                            y += 1

                    case 'RINGGOLD':
                        if RTI_INDEX.count(disOrgID) == 0:
                            del array[x][y]
                            lenArrayX -= 1
                        else:
                            disOrgID_ISNI = RTI[RTI_INDEX.index(disOrgID)][1]

                            if TR_INDEX_ISNI.count(disOrgID_ISNI) == 0:
                                del array[x][y]
                                lenArrayX -= 1
                            else:
                                array = perData_pair_process(TR[TR_INDEX_ISNI.index(disOrgID_ISNI)],array,x,y,TR,TR_INDEX_ROR,16)
                                y += 1

                    case _:
                        del array[x][y]
                        lenArrayX -= 1

            if lenArrayX == 0:
                del array[x]
                lenArray -= 1
            else:
                x += 1

    return array

def perData_sortDate(array):
    return array['StartDate']

def perData_MAIN(RTI,TR,location,fileName,fileFlattenName,cleared):
    xml_files = glob.glob(location)

    n_workers = nWorkers(xml_files)

    ns = {'com':'http://www.orcid.org/ns/common',
        'act':'http://www.orcid.org/ns/activities',
        'edu':'http://www.orcid.org/ns/education',
        'emp':'http://www.orcid.org/ns/employment'}

    perData = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(perData_proc_batch)(batch,ns)
        for batch in tqdm(batch_file(xml_files,n_workers)))

    perDataFlatten = [x for x in flatten(perData,1) if x]
    perDataFlattenLen = len(perDataFlatten)

    n_workers = nWorkers(perDataFlatten)

    RTI_LEN = len(RTI)
    RTI_INDEX = [RTI[i][0] for i in range(0,RTI_LEN)]

    TR_LEN = len(TR)
    TR_INDEX_ROR = [TR[i][0] for i in range(0,TR_LEN)]
    TR_INDEX_FUNDREF = [TR[i][7] for i in range(0,TR_LEN)]
    TR_INDEX_GRID = [TR[i][9] for i in range(0,TR_LEN)]
    TR_INDEX_ISNI = [TR[i][11].replace(' ','') for i in range(0,TR_LEN)]

    perDataPaired = Parallel(n_jobs=n_workers,backend="multiprocessing")(
        delayed(perData_pair_MAIN)(batch,RTI,TR,RTI_INDEX,TR_INDEX_ROR,TR_INDEX_FUNDREF,TR_INDEX_GRID,TR_INDEX_ISNI)
            for batch in tqdm(batch_file(perDataFlatten,n_workers)))

    perDataPairedFlatten = [x for x in flatten(perDataPaired,1) if x]
    perDataPairedFlattenLen = len(perDataPairedFlatten)
    for i in range(perDataPairedFlattenLen):
        perDataPairedFlatten[i].sort(key = perData_sortDate)

    exportFile(perDataPairedFlatten,fileName)
    exportFile([x for x in flatten(perDataPairedFlatten,1) if x],fileFlattenName)
        
    print(cleared)

    return perDataPairedFlatten

def dataFlow_process(perDataElement):
    perDataOrgName = []
    dataFlowProcess = []
    
    for perDataElementX in tqdm(perDataElement):
        lenPerDataElementX = len(perDataElementX)

        for y in range(1,lenPerDataElementX):
            ori = perDataElementX[y-1]
            des = perDataElementX[y]
    
            dataFlowOrigin = ori['DisOrgID']
            dataFlowDestination = des['DisOrgID']
            dataFlowName = ' '.join([dataFlowOrigin,'->',dataFlowDestination])

            if perDataOrgName.count(dataFlowName) == 0:
                perDataOrgName.append(dataFlowName)
                dataFlowProcess.append(
                    {'Count': 1, 'OrgFlow': dataFlowName, 
                    'OriDisOrgID': dataFlowOrigin, 'DesDisOrgID': dataFlowDestination,
                    'OriOrgLocationCountry': ori['OrgLocationCountry'], 'DesOrgLocationCountry': des['OrgLocationCountry'], 
                    'OriOrgLocationDetails': ori['OrgLocationDetails'], 'DesOrgLocationDetails': des['OrgLocationDetails'], 
                    'OriOrgName': ori['OrgName'], 'DesOrgName': des['OrgName'], 
                    'OriOrgType': ori['OrgType'], 'DesOrgType': des['OrgType']})
            else:
                i = perDataOrgName.index(dataFlowName)
                dataFlowProcess[i]['Count'] += 1

    return dataFlowProcess

def dataFlow_sortOrgFlow(array):
    return array['OrgFlow']

def dataFlow_MAIN(perDataElement,fileName,cleared):
    n_workers = nWorkers(perDataElement)
    
    dataFlow = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(dataFlow_process)(batch)
        for batch in tqdm(batch_file(perDataElement,n_workers)))
    
    dataFlowFlatten = [x for x in flatten(dataFlow,1) if x]
    dataFlowFlatten.sort(key = dataFlow_sortOrgFlow)

    x = 1
    lenDataFlow = len(dataFlowFlatten)
    with tqdm(total = lenDataFlow - 1) as pbar:
        while x < lenDataFlow:
            pbar.update(1)
            i = dataFlowFlatten[x]

            if dataFlowFlatten[x-1]['OrgFlow'] == i['OrgFlow']:
                dataFlowFlatten[x-1]['Count'] += i['Count']
                del dataFlowFlatten[x]
                lenDataFlow -= 1
            else:
                x += 1

    exportFile(dataFlowFlatten,fileName)

    print(cleared)

    return dataFlowFlatten

def dataCountOrigin_process(dataFlowElement):
    dataFlowOrgName = []
    dataCountProcess = []

    for x in tqdm(dataFlowElement):
        oriOrgName = x['OriOrgName']
        desOrgName = x['DesOrgName']
        xCount = x['Count']

        i1 = dataFlowOrgName.count(oriOrgName) == 0
        i2 = dataFlowOrgName.count(desOrgName) == 0

        if i1 and i2:
            if oriOrgName == desOrgName:
                dataFlowOrgName.append(oriOrgName)
                dataCountProcess.append({'OrgName': oriOrgName, 'City': x['OriCity'], 'Region': x['OriRegion'], 'Country': x['OriCountry'], 'In': 0, 'Out': 0, 'Self': xCount})
            else:
                dataFlowOrgName.append(oriOrgName)
                dataFlowOrgName.append(desOrgName)
                dataCountProcess.append({'OrgName': oriOrgName, 'City': x['OriCity'], 'Region': x['OriRegion'], 'Country': x['OriCountry'], 'In': 0, 'Out': xCount, 'Self': 0})
                dataCountProcess.append({'OrgName': desOrgName, 'City': x['DesCity'], 'Region': x['DesRegion'], 'Country': x['DesCountry'], 'In': xCount, 'Out': 0, 'Self': 0})

        elif (i1 or i2) is not True:
            if oriOrgName == desOrgName:
                dataCountProcess[dataFlowOrgName.index(oriOrgName)]['Self'] += xCount
            else:
                dataCountProcess[dataFlowOrgName.index(desOrgName)]['In'] += xCount
                dataCountProcess[dataFlowOrgName.index(oriOrgName)]['Out'] += xCount

        elif i1:
            dataFlowOrgName.append(oriOrgName)
            dataCountProcess[dataFlowOrgName.index(desOrgName)]['In'] += xCount
            dataCountProcess.append({'OrgName': oriOrgName, 'City': x['OriCity'], 'Region': x['OriRegion'], 'Country': x['OriCountry'], 'In': 0, 'Out': xCount, 'Self': 0})
        else:
            dataFlowOrgName.append(desOrgName)
            dataCountProcess[dataFlowOrgName.index(oriOrgName)]['Out'] += xCount
            dataCountProcess.append({'OrgName': desOrgName, 'City': x['DesCity'], 'Region': x['DesRegion'], 'Country': x['DesCountry'], 'In': xCount, 'Out': 0, 'Self': 0})

    return dataCountProcess

def dataCountAdjusted_process(dataFlowElement,HDIElement):
    dataFlowOrgName = []
    dataCountProcess = []

    for x in tqdm(dataFlowElement):
        oriOrgName = x['OriOrgName']
        desOrgName = x['DesOrgName']
        oriCouName = x['OriCountry']
        desCouName = x['DesCountry']
        xCount = x['Count']

        yCount = 0
        for y in HDIElement:
            if y['Country'] == oriCouName:
                h1 = float(y['HDI'])
                yCount += 1
            if y['Country'] == desCouName:
                h2 = float(y['HDI'])
                yCount += 1
            if yCount == 2:
                break

        if yCount == 2:
            adjustHDI = h2 / h1
        else:
            adjustHDI = 1

        zCount = xCount * adjustHDI

        i1 = dataFlowOrgName.count(oriOrgName) == 0
        i2 = dataFlowOrgName.count(desOrgName) == 0

        if i1 and i2:
            if oriOrgName == desOrgName:
                dataFlowOrgName.append(oriOrgName)
                dataCountProcess.append({'OrgName': oriOrgName, 'City': x['OriCity'], 'Region': x['OriRegion'], 'Country': x['OriCountry'], 'In': 0, 'Out': 0, 'Self': zCount})
            else:
                dataFlowOrgName.append(oriOrgName)
                dataFlowOrgName.append(desOrgName)
                dataCountProcess.append({'OrgName': oriOrgName, 'City': x['OriCity'], 'Region': x['OriRegion'], 'Country': x['OriCountry'], 'In': 0, 'Out': zCount, 'Self': 0})
                dataCountProcess.append({'OrgName': desOrgName, 'City': x['DesCity'], 'Region': x['DesRegion'], 'Country': x['DesCountry'], 'In': zCount, 'Out': 0, 'Self': 0})

        elif (i1 or i2) is not True:
            if oriOrgName == desOrgName:
                dataCountProcess[dataFlowOrgName.index(oriOrgName)]['Self'] += zCount
            else:
                dataCountProcess[dataFlowOrgName.index(desOrgName)]['In'] += zCount
                dataCountProcess[dataFlowOrgName.index(oriOrgName)]['Out'] += zCount

        elif i1:
            dataFlowOrgName.append(oriOrgName)
            dataCountProcess[dataFlowOrgName.index(desOrgName)]['In'] += zCount
            dataCountProcess.append({'OrgName': oriOrgName, 'City': x['OriCity'], 'Region': x['OriRegion'], 'Country': x['OriCountry'], 'In': 0, 'Out': zCount, 'Self': 0})
        else:
            dataFlowOrgName.append(desOrgName)
            dataCountProcess[dataFlowOrgName.index(oriOrgName)]['Out'] += zCount
            dataCountProcess.append({'OrgName': desOrgName, 'City': x['DesCity'], 'Region': x['DesRegion'], 'Country': x['DesCountry'], 'In': zCount, 'Out': 0, 'Self': 0})

    return dataCountProcess

def dataRankingOrigin_sortRatioOrigin(x):
    return x['RatioOrigin (only external flow)']

def dataRankingAdjusted_sortRatioAdjusted(x):
    return x['RatioAdjusted (only external flow)']

if __name__ == '__main__':
    ID_TO_ROR = readCsv('v1.45.1-2024-04-18-ror-data/v1.45.1-2024-04-18-ror-data_schema_v2.csv')
    ID_RINGGOLD_TO_ISNI = readTsv_ID_RINGGOLD_TO_ISNI('aligned_ringgold_and_isni.tsv')

    perData = perData_MAIN(ID_RINGGOLD_TO_ISNI,ID_TO_ROR,
        'D:/ORCID_2023_10_summaries/*/*.xml','Organization/personal_data.csv','Organization/personal_data_flatten.csv','Done perData')

    # perData = readCsv_perData('Organization/personal_data.csv')

    dataFlow = dataFlow_MAIN(perData,'Organization/organization_flow.csv','Done dataOrgFlow')

    '''
    max_workers = 2 * mp.cpu_count()
    preDataLen = len(dataFlowFlatten)
    n_workers = preDataLen if max_workers > preDataLen else max_workers

    print(f"{n_workers} workers are available")

    dataCountOrigin = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(dataCountOrigin_process)(batch)
        for batch in tqdm(dataCount_batch_file(dataFlowFlatten,n_workers)))

    dataCountOriginFlatten = [x for x in flatten(dataCountOrigin) if x]
    dataCountOriginFlatten.sort(key = dataCount_sortDate)

    x = 1
    lenDataCount = len(dataCountOriginFlatten)
    with tqdm(total = lenDataCount - 1) as pbar:
        while x < lenDataCount:
            pbar.update(1)
            i = dataCountOriginFlatten[x]

            if dataCountOriginFlatten[x-1]['OrgName'] == i['OrgName']:
                dataCountOriginFlatten[x-1]['In'] += i['In']
                dataCountOriginFlatten[x-1]['Out'] += i['Out']
                dataCountOriginFlatten[x-1]['Self'] += i['Self']
                del dataCountOriginFlatten[x]
                lenDataCount -= 1
            else:
                x += 1

    df = pd.DataFrame(dataCountOriginFlatten)
    output_file = os.path.join(directory_path, "dataOrgCountOrigin.csv")
    df.to_csv(output_file,index=False,encoding='utf-8-sig')

    print(df)
    print("Data exported to", output_file)
    print('Stage 2 cleared')
    '''

    '''
    with open('HDI.csv') as f:
        HDI = [{k: v 
            for k, v in row.items()}
                for row in csv.DictReader(f, skipinitialspace=True)]

    max_workers = 2 * mp.cpu_count()
    preDataLen = len(dataFlowFlatten)
    n_workers = preDataLen if max_workers > preDataLen else max_workers

    print(f"{n_workers} workers are available")

    dataCountAdjusted = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(dataCountAdjusted_process)(batch,HDI)
        for batch in tqdm(dataCount_batch_file(dataFlowFlatten,n_workers)))

    dataCountAdjustedFlatten = [x for x in flatten(dataCountAdjusted) if x]
    dataCountAdjustedFlatten.sort(key = dataCount_sortDate)

    x = 1
    lenDataCount = len(dataCountAdjustedFlatten)
    with tqdm(total = lenDataCount - 1) as pbar:
        while x < lenDataCount:
            pbar.update(1)
            i = dataCountAdjustedFlatten[x]

            if dataCountAdjustedFlatten[x-1]['OrgName'] == i['OrgName']:
                dataCountAdjustedFlatten[x-1]['In'] += i['In']
                dataCountAdjustedFlatten[x-1]['Out'] += i['Out']
                dataCountAdjustedFlatten[x-1]['Self'] += i['Self']
                del dataCountAdjustedFlatten[x]
                lenDataCount -= 1
            else:
                x += 1

    df = pd.DataFrame(dataCountAdjustedFlatten)
    output_file = os.path.join(directory_path, "dataOrgCountAdjusted.csv")
    df.to_csv(output_file,index=False,encoding='utf-8-sig')

    print(df)
    print("Data exported to", output_file)
    print('Stage 3 cleared')
    '''