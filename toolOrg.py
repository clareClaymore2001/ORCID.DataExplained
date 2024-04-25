import os,time,random
import xml.etree.ElementTree as ET
import pandas as pd
import glob
import csv

import multiprocessing as mp
import concurrent.futures
from tqdm import tqdm
from joblib import Parallel, delayed

import SpringRank as sr
import networkx as nx
import numpy as np
import tools as tl

from rapidfuzz import process, fuzz, distance, utils

from pycirclize import Circos

def readCsv(location):
    with open(location,encoding='utf-8-sig') as file:
        csv_file = csv.reader(file)
        next(csv_file)
        return [line for line in csv_file]

def readCsv_roleTitle(location):
    result = {}

    with open(location,encoding='utf-8-sig') as file:
        csv_file = csv.reader(file)

        for row in csv_file:
            lenRow = len(row)
            row0 = str(row[0])
            result[row0] = [str(row[i]) for i in range(1,lenRow) if row[i] != '']

    return(result)

def readCsv_perData(location):
    result = []

    with open(location,encoding='utf-8-sig') as file:
        csv_file = csv.reader(file)
        next(csv_file)

        for row in tqdm(csv_file):
            resultRow = []

            for item in row:
                if item != '':
                    resultRow.append(eval(item))

            result.append(resultRow)

    return result

def readCsv_asDict(location):
    return (pd.read_csv(location)).to_dict(orient='records')
    
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

def perData_raw_process(xml_file,ns):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    perDataProcess = []

    for actSummary in root.findall('act:activities-summary',ns):
        for edu in actSummary.findall('act:educations',ns):
            for eduAffiliationGroup in edu.findall('act:affiliation-group',ns):
                for eduSummary in eduAffiliationGroup.findall('edu:education-summary',ns):
                    eduStartDate = 0
                    eduDisOrgID = eduDisSource = eduDepartmentName = eduRoleTitle = eduOrgName = eduOrgCountry = eduOrgCity = None

                    for eduRoleTitleElement in eduSummary.findall('com:role-title',ns):
                        eduRoleTitle = eduRoleTitleElement.text

                    for eduDepartmentNameElement in eduSummary.findall('com:department-name',ns):
                        eduDepartmentName = eduDepartmentNameElement.text

                    for eduStartDateElement in eduSummary.findall('com:start-date',ns):
                        for eduStartDateYearElement in eduStartDateElement.findall('com:year',ns):
                            eduStartDate += 10000 * int(eduStartDateYearElement.text)
                        for eduStartDateMonthElement in eduStartDateElement.findall('com:month',ns):
                            eduStartDate += 100 * int(eduStartDateMonthElement.text)
                        for eduStartDateDayElement in eduStartDateElement.findall('com:day',ns):
                            eduStartDate += int(eduStartDateDayElement.text)

                    for eduOrg in eduSummary.findall('com:organization',ns):
                        eduOrgName = eduOrg.find('com:name',ns).text

                        for eduAddress in eduOrg.findall('com:address',ns):
                            eduOrgCountry = eduAddress.find('com:country',ns).text
                            eduOrgCity = eduAddress.find('com:city',ns).text

                        for eduDisOrg in eduOrg.findall('com:disambiguated-organization',ns):
                            for eduDisSourceElement in eduDisOrg.findall('com:disambiguation-source',ns):
                                eduDisSource = (eduDisSourceElement.text)
                            for eduDisOrgIDElement in eduDisOrg.findall('com:disambiguated-organization-identifier',ns):
                                eduDisOrgID = (eduDisOrgIDElement.text)

                    if eduStartDate != 0 and ((eduDisOrgID is not None and eduDisSource is not None) or (eduOrgName is not None and eduOrgCountry is not None and eduOrgCity is not None)):
                        perDataProcess.append({
                            'StartDate': eduStartDate, 'DisSource': eduDisSource, 'DisOrgID': eduDisOrgID, 
                            'DepartmentName': eduDepartmentName, 'RoleTitle': eduRoleTitle, 'OrgName': eduOrgName, 
                            'OrgCountry': eduOrgCountry, 'OrgCity': eduOrgCity})

        for emp in actSummary.findall('act:employments',ns):
            for empAffiliationGroup in emp.findall('act:affiliation-group',ns):
                for empSummary in empAffiliationGroup.findall('emp:employment-summary',ns):
                    empStartDate = 0
                    empDisOrgID = empDisSource = empDepartmentName = empRoleTitle = empOrgName = empOrgCity = None

                    for empRoleTitleElement in empSummary.findall('com:role-title',ns):
                        empRoleTitle = empRoleTitleElement.text

                    for empDepartmentNameElement in empSummary.findall('com:department-name',ns):
                        empDepartmentName = empDepartmentNameElement.text

                    for empStartDateElement in empSummary.findall('com:start-date',ns):
                        for empStartDateYearElement in empStartDateElement.findall('com:year',ns):
                            empStartDate += 10000 * int(empStartDateYearElement.text)
                        for empStartDateMonthElement in empStartDateElement.findall('com:month',ns):
                            empStartDate += 100 * int(empStartDateMonthElement.text)
                        for empStartDateDayElement in empStartDateElement.findall('com:day',ns):
                            empStartDate += int(empStartDateDayElement.text)

                    for empOrg in empSummary.findall('com:organization',ns):
                        empOrgName = empOrg.find('com:name',ns).text

                        for empAddress in empOrg.findall('com:address',ns):
                            empOrgCountry = empAddress.find('com:country',ns).text
                            empOrgCity = empAddress.find('com:city',ns).text

                        for empDisOrg in empOrg.findall('com:disambiguated-organization',ns):
                            for empDisSourceElement in empDisOrg.findall('com:disambiguation-source',ns):
                                empDisSource = (empDisSourceElement.text)
                            for empDisOrgIDElement in empDisOrg.findall('com:disambiguated-organization-identifier',ns):
                                empDisOrgID = (empDisOrgIDElement.text)

                    if empStartDate != 0 and ((empDisOrgID is not None and empDisSource is not None) or (empOrgName is not None and empOrgCountry is not None and empOrgCity is not None)):
                        perDataProcess.append({
                            'StartDate': empStartDate, 'DisSource': empDisSource, 'DisOrgID': empDisOrgID, 
                            'DepartmentName': empDepartmentName, 'RoleTitle': empRoleTitle, 'OrgName': empOrgName, 
                            'OrgCountry': empOrgCountry, 'OrgCity': empOrgCity})

    return perDataProcess

def perData_raw_proc_batch(batch,ns):
  return [perData_raw_process(xml_file,ns)
    for xml_file in tqdm(batch)]

def perData_raw_sortDate(array):
    return array['StartDate']

def perData_raw_MAIN(RTI,TR,xml_files,fileName,fileFlattenName,cleared):
    n_workers = nWorkers(xml_files)

    ns = {'com':'http://www.orcid.org/ns/common',
        'act':'http://www.orcid.org/ns/activities',
        'edu':'http://www.orcid.org/ns/education',
        'emp':'http://www.orcid.org/ns/employment'}

    perData = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(perData_raw_proc_batch)(batch,ns)
        for batch in tqdm(batch_file(xml_files,n_workers)))

    perDataFlatten = [x for x in flatten(perData,1) if x]
    perDataFlattenLen = len(perDataFlatten)
    for i in range(perDataFlattenLen):
        perDataFlatten[i].sort(key = perData_raw_sortDate)

    # exportFile(perDataFlatten,fileName)
    # exportFile([x for x in flatten(perDataFlatten,1) if x],fileFlattenName)

    print(cleared)

    return perDataFlatten

def perData_pair_FUNDREF(string,string_list):
    for item in string_list:
        if string in item:
            return item
    return -1

def perData_pair_modify(i,array,x,y):
    del array[x][y]['DisSource']
    del array[x][y]['OrgCountry']
    del array[x][y]['OrgCity']
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

def perData_pair_proc_batch(array,RTI,TR,RTI_INDEX,TR_INDEX_ROR,TR_INDEX_FUNDREF,TR_INDEX_GRID,TR_INDEX_ISNI,TR_INDEX_NAME,fill):
    lenTIN = len(TR_INDEX_NAME)

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
                            array[x][y]['DisSource'] = 'case _'
                        else:
                            array = perData_pair_process(TR[TR_INDEX_ROR.index(disOrgID)],array,x,y,TR,TR_INDEX_ROR,16)
                            y += 1

                    case 'FUNDREF':
                        disOrgID = arrayXY['DisOrgID'][27:]
                        j = perData_pair_FUNDREF(disOrgID,TR_INDEX_FUNDREF)

                        if j == -1:
                            array[x][y]['DisSource'] = 'case _'
                        else:
                            array = perData_pair_process(TR[TR_INDEX_FUNDREF.index(j)],array,x,y,TR,TR_INDEX_ROR,16)
                            y += 1

                    case 'GRID':
                        if TR_INDEX_GRID.count(disOrgID) == 0:
                            array[x][y]['DisSource'] = 'case _'
                        else:
                            array = perData_pair_process(TR[TR_INDEX_GRID.index(disOrgID)],array,x,y,TR,TR_INDEX_ROR,16)
                            y += 1

                    case 'RINGGOLD':
                        if RTI_INDEX.count(disOrgID) == 0:
                            array[x][y]['DisSource'] = 'case _'
                        else:
                            disOrgID_ISNI = RTI[RTI_INDEX.index(disOrgID)][1]

                            if TR_INDEX_ISNI.count(disOrgID_ISNI) == 0:
                                array[x][y]['DisSource'] = 'case _'
                            else:
                                array = perData_pair_process(TR[TR_INDEX_ISNI.index(disOrgID_ISNI)],array,x,y,TR,TR_INDEX_ROR,16)
                                y += 1

                    case _:
                        orgName = arrayXY['OrgName']

                        if orgName == '':
                            del array[x][y]
                            lenArrayX -= 1
                        else:
                            if TR_INDEX_NAME.count(orgName) == 0:
                                if fill:
                                    pairNameQ = process.extractOne(orgName,TR_INDEX_NAME,scorer=fuzz.QRatio,processor=utils.default_process,score_cutoff=86)

                                    if pairNameQ is None:
                                        pairNameW = process.extractOne(orgName,TR_INDEX_NAME,scorer=fuzz.WRatio,processor=utils.default_process,score_cutoff=94)

                                        if pairNameW is None:
                                            del array[x][y]
                                            lenArrayX -= 1
                                        else:
                                            i = pairNameW[2]

                                            if arrayXY['OrgCountry'] == TR[i][18]:
                                                array = perData_pair_process(TR[i],array,x,y,TR,TR_INDEX_ROR,16)
                                                array[x][y]['PairType'] = 'W'
                                                array[x][y]['PairOrgNameOri'] = orgName
                                                array[x][y]['PairOrgNamePair'] = pairNameW[0]
                                                array[x][y]['PairOrgNameValue'] = pairNameW[1]
                                                y += 1
                                            else:
                                                del array[x][y]
                                                lenArrayX -= 1

                                    else:
                                        v = pairNameQ[1]
                                        i = pairNameQ[2]

                                        if v > 94:
                                            array = perData_pair_process(TR[i],array,x,y,TR,TR_INDEX_ROR,16)
                                            array[x][y]['PairType'] = 'Q'
                                            array[x][y]['PairOrgNameOri'] = orgName
                                            array[x][y]['PairOrgNamePair'] = pairNameQ[0]
                                            array[x][y]['PairOrgNameValue'] = v
                                            y += 1
                                        else:
                                            if arrayXY['OrgCountry'] == TR[i][18]:
                                                orgCity = arrayXY['OrgCity']
                                                pairCity = fuzz.WRatio(orgCity,TR[i][22],processor=utils.default_process)

                                                if v + pairCity > 173:
                                                    array = perData_pair_process(TR[i],array,x,y,TR,TR_INDEX_ROR,16)
                                                    array[x][y]['PairType'] = 'Q'
                                                    array[x][y]['PairOrgNameOri'] = orgName
                                                    array[x][y]['PairOrgNamePair'] = pairNameQ[0]
                                                    array[x][y]['PairOrgNameValue'] = v
                                                    array[x][y]['PairCItyOri'] = orgCity
                                                    array[x][y]['PairCityPair'] = TR[i][22]
                                                    array[x][y]['PairCityValue'] = pairCity
                                                    y += 1
                                                else:
                                                    del array[x][y]
                                                    lenArrayX -= 1

                                            else:
                                                del array[x][y]
                                                lenArrayX -= 1

                                else:
                                    del array[x][y]
                                    lenArrayX -= 1

                            else:
                                i = TR_INDEX_NAME.index(orgName)

                                if arrayXY['OrgCountry'] == TR[i][18]:
                                    array = perData_pair_process(TR[i],array,x,y,TR,TR_INDEX_ROR,16)
                                    y += 1
                                else:
                                    del array[x][y]
                                    lenArrayX -= 1

            if lenArrayX == 0:
                del array[x]
                lenArray -= 1
            else:
                x += 1

    return array

def perData_pair_MAIN(perDataElement,RTI,TR,fileName,fileFlattenName,cleared,fill):
    n_workers = nWorkers(perDataElement)

    RTI_LEN = len(RTI)
    RTI_INDEX = [RTI[i][0] for i in range(0,RTI_LEN)]

    TR_LEN = len(TR)
    TR_INDEX_ROR = [TR[i][0] for i in range(0,TR_LEN)]
    TR_INDEX_FUNDREF = [TR[i][7] for i in range(0,TR_LEN)]
    TR_INDEX_GRID = [TR[i][9] for i in range(0,TR_LEN)]
    TR_INDEX_ISNI = [TR[i][11].replace(' ','') for i in range(0,TR_LEN)]
    TR_INDEX_NAME = [TR[i][26] for i in range(0,TR_LEN)]

    perDataPaired = Parallel(n_jobs=n_workers,backend="multiprocessing")(
        delayed(perData_pair_proc_batch)(batch,RTI,TR,RTI_INDEX,TR_INDEX_ROR,TR_INDEX_FUNDREF,TR_INDEX_GRID,TR_INDEX_ISNI,TR_INDEX_NAME,fill)
            for batch in tqdm(batch_file(perDataElement,n_workers)))

    perDataPairedFlatten = [x for x in flatten(perDataPaired,1) if x]

    exportFile(perDataPairedFlatten,fileName)
    exportFile([x for x in flatten(perDataPairedFlatten,1) if x],fileFlattenName)
        
    print(cleared)

    return perDataPairedFlatten

def perDataRoleTitled_process(array,RI):
    RI_keys = [key for key in RI.keys()]
    lenRI = len(RI_keys)

    x = 0
    lenArray = len(array)

    with tqdm(total = lenArray) as pbar:
        while x < lenArray:
            pbar.update(1)
            arrayX = array[x]

            y = 0
            lenArrayX = len(arrayX)
            while y < lenArrayX:
                roletitle = arrayX[y]['RoleTitle']

                if roletitle is None:
                    del array[x][y]
                    lenArrayX -= 1
                else:
                    pair = [process.extractOne(roletitle,RI[key],scorer=fuzz.partial_ratio) for key in RI_keys]
                    pairValue = [pair[i][1] for i in range(lenRI)]
                    max_value = max(pairValue)

                    if max_value > 86:
                        array[x][y]['RoleTitle'] = RI_keys[pairValue.index(max_value)]
                        array[x][y]['RoleTitleOri'] = roletitle
                        array[x][y]['RoleTitlePair'] = pair
                        y += 1
                    else:
                        del array[x][y]
                        lenArrayX -= 1

            if lenArrayX == 0:
                del array[x]
                lenArray -= 1
            else:
                x += 1

    return array

def perDataRoleTitled_MAIN(array,RI,fileName,fileFlattenName,cleared):
    n_workers = nWorkers(array)

    perDataRoleTitledFlatten = perDataRoleTitled_process(array,RI)

    # perDataRoleTitled = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(perDataRoleTitled_process)(batch,RI)
    #     for batch in tqdm(batch_file(array,n_workers)))

    # perDataRoleTitledFlatten = [x for x in flatten(perDataRoleTitled,1) if x]

    exportFile(perDataRoleTitledFlatten,fileName)
    exportFile([x for x in flatten(perDataRoleTitledFlatten,1) if x],fileFlattenName)
        
    print(cleared)

    return perDataRoleTitledFlatten

def perDataDepted_process(array,DI):
    DI_keys = [key for key in DI.keys()]
    lenDI = len(DI_keys)

    x = 0
    lenArray = len(array)

    with tqdm(total = lenArray) as pbar:
        while x < lenArray:
            pbar.update(1)
            arrayX = array[x]

            y = 0
            lenArrayX = len(arrayX)
            while y < lenArrayX:
                deptname = arrayX[y]['DepartmentName']

                if deptname is None:
                    del array[x][y]
                    lenArrayX -= 1
                else:
                    pair = [process.extractOne(deptname,DI[key],scorer=fuzz.partial_ratio,processor=utils.default_process) for key in DI_keys]
                    pairValue = [pair[i][1] for i in range(lenDI)]
                    max_value = max(pairValue)

                    if max_value > 86:
                        array[x][y]['DepartmentName'] = DI_keys[pairValue.index(max_value)]
                        array[x][y]['DepartmentNameOri'] = deptname
                        array[x][y]['DepartmentNamePair'] = pair
                        y += 1
                    else:
                        del array[x][y]
                        lenArrayX -= 1

            if lenArrayX == 0:
                del array[x]
                lenArray -= 1
            else:
                x += 1

    return array

def perDataDepted_MAIN(array,DI,fileName,fileFlattenName,cleared):
    n_workers = nWorkers(array)

    # perDataDeptedFlatten = perDataDepted_process(array,DI)

    perDataDepted = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(perDataDepted_process)(batch,DI)
        for batch in tqdm(batch_file(array,n_workers)))

    perDataDeptedFlatten = [x for x in flatten(perDataDepted,1) if x]

    exportFile(perDataDeptedFlatten,fileName)
    exportFile([x for x in flatten(perDataDeptedFlatten,1) if x],fileFlattenName)
        
    print(cleared)

    return perDataDeptedFlatten

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

def dataFlowRoleTitled_process_modify(dataFlowProcessElement,perDataFlowFrom,perDataFlowTo):
    perDataOrgName = dataFlowProcessElement[0]
    dataFlowProcess = dataFlowProcessElement[1]

    for ori in perDataFlowFrom:
        oriV = ori[0]
        oriR = ori[1]

        for des in perDataFlowTo:
            desV = des[0]
            desR = des[1]

            if oriR == desR and oriV['StartDate'] < desV['StartDate']:
                if oriR == 'Y':oriV,desV = desV,oriV

                dataFlowOrigin = oriV['DisOrgID']
                dataFlowDestination = desV['DisOrgID']
                dataFlowName = ' '.join([dataFlowOrigin,'->',dataFlowDestination])

                if perDataOrgName.count(dataFlowName) == 0:
                    perDataOrgName.append(dataFlowName)
                    dataFlowProcess.append(
                        {'Count': 1, 'OrgFlow': dataFlowName, 
                        'OriDisOrgID': dataFlowOrigin, 'DesDisOrgID': dataFlowDestination,
                        'OriOrgLocationCountry': oriV['OrgLocationCountry'], 'DesOrgLocationCountry': desV['OrgLocationCountry'], 
                        'OriOrgLocationDetails': oriV['OrgLocationDetails'], 'DesOrgLocationDetails': desV['OrgLocationDetails'], 
                        'OriOrgName': oriV['OrgName'], 'DesOrgName': desV['OrgName'], 
                        'OriOrgType': oriV['OrgType'], 'DesOrgType': desV['OrgType'],})
                else:
                    i = perDataOrgName.index(dataFlowName)
                    dataFlowProcess[i]['Count'] += 1

    return [perDataOrgName,dataFlowProcess]

def dataFlowRoleTitled_process(perDataElement,type,country,RT):
    flowFrom = []
    flowTo = []
    flowReverse = []

    for item in RT:
        flowFrom.append(item[0])
        flowTo.append(item[1])
        flowReverse.append(item[2])

    lenRT = len(flowReverse)

    dataFlowProcess = [[],[]]

    for perDataElementX in tqdm(perDataElement):
        lenPerDataElementX = len(perDataElementX)

        for RT_item in range(lenRT):
            perDataFlowFrom = []
            perDataFlowTo = []

            itemFrom = flowFrom[RT_item]
            itemTo = flowTo[RT_item]
            itemReverse = flowReverse[RT_item]

            for y in range(0,lenPerDataElementX):
                perDataElementXY = perDataElementX[y]

                if perDataElementXY['OrgType'] in type and perDataElementXY['OrgLocationCountry'] in country:
                    XY_roleTitle = perDataElementXY['RoleTitle']

                    if XY_roleTitle == itemFrom:perDataFlowFrom.append([perDataElementXY,itemReverse])
                    if XY_roleTitle == itemTo:perDataFlowTo.append([perDataElementXY,itemReverse])

            dataFlowProcess = dataFlowRoleTitled_process_modify(dataFlowProcess,perDataFlowFrom,perDataFlowTo)

    return dataFlowProcess[1]

def dataFlowRoleTitled_MAIN(perDataElement,type,country,RT,fileName,cleared):
    n_workers = nWorkers(perDataElement)

    # dataFlowFlatten = dataFlowRoleTitled_process(perDataElement,type,country,RT)

    dataFlow = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(dataFlowRoleTitled_process)(batch,type,country,RT)
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

def dataCount_process(dataFlowElement):
    dataFlowOrgID = []
    dataCountProcess = []

    for x in tqdm(dataFlowElement):
        oriOrgID = x['OriDisOrgID']
        desOrgID = x['DesDisOrgID']
        xCount = x['Count']

        i1 = dataFlowOrgID.count(oriOrgID) == 0
        i2 = dataFlowOrgID.count(desOrgID) == 0

        if i1 and i2:
            if oriOrgID == desOrgID:
                dataFlowOrgID.append(oriOrgID)
                dataCountProcess.append({'OrgID': oriOrgID, 
                    'OrgLocationCountry': x['OriOrgLocationCountry'], 
                    'OrgLocationDetails': x['OriOrgLocationDetails'], 
                    'OrgName': x['OriOrgName'], 
                    'OrgType': x['OriOrgType'], 
                    'In': 0, 'Out': 0, 'Self': xCount})
            else:
                dataFlowOrgID.append(oriOrgID)
                dataFlowOrgID.append(desOrgID)
                dataCountProcess.append({'OrgID': oriOrgID, 
                    'OrgLocationCountry': x['OriOrgLocationCountry'], 
                    'OrgLocationDetails': x['OriOrgLocationDetails'], 
                    'OrgName': x['OriOrgName'], 
                    'OrgType': x['OriOrgType'], 
                    'In': 0, 'Out': xCount, 'Self': 0})
                dataCountProcess.append({'OrgID': desOrgID, 
                    'OrgLocationCountry': x['DesOrgLocationCountry'], 
                    'OrgLocationDetails': x['DesOrgLocationDetails'], 
                    'OrgName': x['DesOrgName'], 
                    'OrgType': x['DesOrgType'], 
                    'In': xCount, 'Out': 0, 'Self': 0})

        elif (i1 or i2) is not True:
            if oriOrgID == desOrgID:
                dataCountProcess[dataFlowOrgID.index(oriOrgID)]['Self'] += xCount
            else:
                dataCountProcess[dataFlowOrgID.index(desOrgID)]['In'] += xCount
                dataCountProcess[dataFlowOrgID.index(oriOrgID)]['Out'] += xCount

        elif i1:
            dataFlowOrgID.append(oriOrgID)
            dataCountProcess[dataFlowOrgID.index(desOrgID)]['In'] += xCount
            dataCountProcess.append({'OrgID': oriOrgID, 
                'OrgLocationCountry': x['OriOrgLocationCountry'], 
                'OrgLocationDetails': x['OriOrgLocationDetails'], 
                'OrgName': x['OriOrgName'], 
                'OrgType': x['OriOrgType'], 
                'In': 0, 'Out': xCount, 'Self': 0})
        else:
            dataFlowOrgID.append(desOrgID)
            dataCountProcess[dataFlowOrgID.index(oriOrgID)]['Out'] += xCount
            dataCountProcess.append({'OrgID': desOrgID, 
                'OrgLocationCountry': x['DesOrgLocationCountry'], 
                'OrgLocationDetails': x['DesOrgLocationDetails'], 
                'OrgName': x['DesOrgName'], 
                'OrgType': x['DesOrgType'], 
                'In': xCount, 'Out': 0, 'Self': 0})

    return dataCountProcess

def dataCount_sortOrgID(array):
    return array['OrgID']

def dataCount_MAIN(dataFlowElement,fileName,cleared):
    n_workers = nWorkers(dataFlowElement)

    dataCount = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(dataCount_process)(batch)
        for batch in tqdm(batch_file(dataFlowElement,n_workers)))

    dataCountFlatten = [x for x in flatten(dataCount,1) if x]
    dataCountFlatten.sort(key = dataCount_sortOrgID)

    x = 1
    lenDataCount = len(dataCountFlatten)
    with tqdm(total = lenDataCount - 1) as pbar:
        while x < lenDataCount:
            pbar.update(1)
            i = dataCountFlatten[x]

            if dataCountFlatten[x-1]['OrgID'] == i['OrgID']:
                dataCountFlatten[x-1]['In'] += i['In']
                dataCountFlatten[x-1]['Out'] += i['Out']
                dataCountFlatten[x-1]['Self'] += i['Self']
                del dataCountFlatten[x]
                lenDataCount -= 1
            else:
                x += 1

    exportFile(dataCountFlatten,fileName)

    print(cleared)

def dataSpringRank_process_build_graph_from_adjacency(inadjacency,type,country):
    edges={}

    if type is None and country is None:
        for row in tqdm(inadjacency):
            edges[(row['OriDisOrgID'],row['DesDisOrgID'])] = int(row['Count'])
    elif type is not None and country is None:
        for row in tqdm(inadjacency):
            if row['OriOrgType'] in type and row['DesOrgType'] in type:
                edges[(row['OriDisOrgID'],row['DesDisOrgID'])] = int(row['Count'])
    elif type is None and country is not None:
        for row in tqdm(inadjacency):
            if str(row['OriOrgLocationCountry']) in country and str(row['DesOrgLocationCountry']) in country:
                edges[(row['OriDisOrgID'],row['DesDisOrgID'])] = int(row['Count'])
    else:
        for row in tqdm(inadjacency):
            if (row['OriOrgType'] in type and row['DesOrgType'] in type) and (str(row['OriOrgLocationCountry']) in country and str(row['DesOrgLocationCountry']) in country):
                edges[(row['OriDisOrgID'],row['DesDisOrgID'])] = int(row['Count'])

    if edges == {}:return None
    else:
        G = nx.DiGraph()

        for e in edges:
            G.add_edge(e[0],e[1],weight=edges[e])

        return G

def dataSpringRank_process(dataFlowElement,type,country):
    G = dataSpringRank_process_build_graph_from_adjacency(dataFlowElement,type,country)

    if G is None:return []
    else:
        nodes = list(G.nodes())
        A = nx.to_scipy_sparse_array(G, dtype=float, nodelist=nodes)
        rank = sr.get_ranks(A)

        X = [(nodes[i],rank[i]) for i in range(G.number_of_nodes())]
        X = sorted(X, key=lambda tup: tup[1],reverse=True)

        return X

def dataSpringRank_MAIN(dataFlowElement,type,country,TR,fileName,cleared):
    dataSpringRank = dataSpringRank_process(dataFlowElement,type,country)

    if dataSpringRank != []:
        TR_LEN = len(TR)
        TR_INDEX = [TR[i][0] for i in range(0,TR_LEN)]
    
        lenDataSpringRank = len(dataSpringRank)
        for x in tqdm(range(lenDataSpringRank)):
            dataSpringRankX = dataSpringRank[x]
            dataSpringRankX0 = dataSpringRankX[0]
            dataSpringRankX0_TR = TR[TR_INDEX.index(dataSpringRankX0)]
            dataSpringRank[x] = {
                'OrgSpringRank': dataSpringRankX[1], 
                'OrgID': dataSpringRankX0, 
                'OrgLocationCountry': dataSpringRankX0_TR[18], 
                'OrgLocationDetails': dataSpringRankX0_TR[22], 
                'OrgName': dataSpringRankX0_TR[26], 
                'OrgType': dataSpringRankX0_TR[30]}

    exportFile(dataSpringRank,fileName)

    print(cleared)

    return dataSpringRank

def generate_SpringRank_process(RT,type,country,ID_TO_ROR,perDataRoleTitled,R,T,C):
    pathOrg = 'data/organization/'
    pathOrgPer = pathOrg + 'person/'
    pathOrgSpringRank = pathOrg + 'SpringRank/'

    path = ''

    if T:path += 'alltypes/'
    else:path += type[0] + '/'

    if R:path += 'allflows/'
    else:path += RT[0][0] + '_to_' + RT[0][1] + '/'

    if C:path += 'allcountries/'
    else:path += country[0] + '/'

    try:os.makedirs(os.path.join(os.getcwd(),pathOrgSpringRank,path))
    except:print()

    if RT is None:RT = readCsv('source/role_title_fromto.csv')
    if type is None:type = readCsv('source/organization_type.csv')[0]
    if country is None:country = readCsv('source/country.csv')[0]

    if ID_TO_ROR is None:ID_TO_ROR = readCsv('source/ROR_data.csv')

    if perDataRoleTitled is None:perDataRoleTitled = readCsv_perData(pathOrgPer+'personal_data_depted.csv')

    dataFlowRoleTitled = dataFlowRoleTitled_MAIN(perDataRoleTitled,type,country,RT,pathOrgSpringRank+path+'organization_flow_depted.csv','Done dataOrgFlow')
    dataSpringRank_MAIN(dataFlowRoleTitled,type,country,ID_TO_ROR,pathOrgSpringRank+path+'organization_SpringRank_depted.csv','Done dataOrgSpringRank')
    generate_chordDiagram_MAIN(pathOrgSpringRank+path,'Done chordDiagram')

def generate_SpringRank_MAIN(RT,type,country,R,T,C):
    pathOrg = 'data/organization/'
    pathOrgPer = pathOrg + 'person/'
    pathOrgSpringRank = pathOrg + 'SpringRank/'

    ID_TO_ROR = readCsv('source/ROR_data.csv')
    RT_FROMTO = readCsv('source/role_title_fromto.csv')
    ORG_TYPES = readCsv('source/organization_type.csv')[0]
    ORG_COUNTRIES = readCsv('source/country.csv')[0]

    if RT == 'All':RT = RT_FROMTO
    if type == 'All': type = ORG_TYPES
    if country == 'All': country = ORG_COUNTRIES

    perDataRoleTitled = readCsv_perData(pathOrgPer+'personal_data_depted.csv')

    for RTElement in RT:
        for typeElement in type:
            for countryElement in country:
                generate_SpringRank_process([RTElement],typeElement,countryElement,ID_TO_ROR,perDataRoleTitled,False,False,False)
                print('Done',RTElement,typeElement,countryElement)

            if C:generate_SpringRank_process([RTElement],typeElement,None,ID_TO_ROR,perDataRoleTitled,False,False,True)

        if T:
            if C:generate_SpringRank_process([RTElement],None,None,ID_TO_ROR,perDataRoleTitled,False,True,True)
            else:generate_SpringRank_process([RTElement],None,country,ID_TO_ROR,perDataRoleTitled,False,True,False)

    if R:
        if T:
            if C:generate_SpringRank_process(RT_FROMTO,None,None,ID_TO_ROR,perDataRoleTitled,True,True,True)
            else:generate_SpringRank_process(RT_FROMTO,None,country,ID_TO_ROR,perDataRoleTitled,True,True,False)
        else:
            if C:generate_SpringRank_process(RT_FROMTO,type,None,ID_TO_ROR,perDataRoleTitled,True,False,True)
            else:generate_SpringRank_process(RT_FROMTO,type,country,ID_TO_ROR,perDataRoleTitled,True,False,False)\

def generate_chordDiagram_cut(cutFrom,cutTo,flow,SpringRank):
    lenSpringRank = len(SpringRank)
    lenCutFrom = round(len(SpringRank) * cutFrom / 100)
    lenCutTo = round(len(SpringRank) * cutTo / 100)

    names = []
    for i in tqdm(range(lenCutFrom,lenCutTo)):names.append(SpringRank[i][4])

    matrix_data = np.array([[0 for i in range(lenCutFrom,lenCutTo)] for j in tqdm(range(lenCutFrom,lenCutTo))])
    for row in tqdm(flow):
        item1 = row[8]
        item2 = row[9]

        if item1 in names and item2 in names:
            i1 = names.index(item1)
            i2 = names.index(item2)

            if i1 != i2: matrix_data[i1][i2] = int(row[0])

    return [names,matrix_data]

def generate_chordDiagram_process(pieces,lenNames,names,matrix_data):
    if lenNames > pieces:
        matrix_sum = [[],[]]

        for i in range(2):
            for row in tqdm(matrix_data):
                matrix_sum[i].append(sum(row))

            matrix_data.T

        initial = 0
        limit = 1
        lenNamesFiltered = len(names)

        with tqdm(total = lenNames - pieces) as pbar:
            while lenNamesFiltered > pieces:
                limit += 1
                initial = lenNamesFiltered
                lenNamesFiltered1 = len(list(filter(lambda x:x>=limit,matrix_sum[0])))
                lenNamesFiltered2 = len(list(filter(lambda x:x>=limit,matrix_sum[1])))
                lenNamesFiltered = max(lenNamesFiltered1,lenNamesFiltered2)
                pbar.update(initial-lenNamesFiltered)

            if lenNamesFiltered < 2:
                limit -= 1
                lenNamesFiltered1 = len(list(filter(lambda x:x>=limit,matrix_sum[0])))
                lenNamesFiltered2 = len(list(filter(lambda x:x>=limit,matrix_sum[1])))
                lenNamesFiltered = max(lenNamesFiltered1,lenNamesFiltered2)

        print('Limit:',limit)
        print('Filtered:',lenNamesFiltered)

        names_final = []
        matrix_data_final = np.array([[0 for i in range(lenNamesFiltered)] for j in tqdm(range(lenNamesFiltered))])

        with tqdm(total = lenNamesFiltered ** 2) as pbar:
            x = 0

            for i in range(lenNames):
                if matrix_sum[0][i] >= limit and matrix_sum[1][i] >= limit:
                    names_final.append(names[i])
                    y = 0

                    for j in range(lenNames):
                        if matrix_sum[0][j] >= limit and matrix_sum[1][j] >= limit:
                            matrix_data_final[x][y] = matrix_data[i][j]
                            y += 1
                            pbar.update(1)
                    
                    x += 1

    else:
        names_final = names
        matrix_data_final = matrix_data

    return [names_final,matrix_data_final]

def generate_chordDiagram_MAIN(path,cleared):
    flow = readCsv(path+'organization_flow_depted.csv')
    SpringRank = readCsv(path+'organization_SpringRank_depted.csv')

    retult = generate_chordDiagram_cut(0,15,flow,SpringRank)

    names = retult[0]
    matrix_data = retult[1]

    lenNames = len(names)

    result = generate_chordDiagram_process(90,lenNames,names,matrix_data)

    names = result[0]
    matrix_data = result[1]

    matrix_df = pd.DataFrame(matrix_data,index=names,columns=names)

    circos = Circos.initialize_from_matrix(
        matrix_df,
        space=0,
        r_lim =(99,100),
        cmap = 'jet',
        label_kws=dict(r=101, size=4, color="black", orientation="vertical"),)

    circos.savefig(path+'chordDiagram_depted.svg')

    print(cleared)

if __name__ == '__main__':
    # generate_SpringRank_process(None,None,['JP'],None,None,True,True,False)

    # generate_SpringRank_MAIN([],[],[],True,True,True)

    # generate_SpringRank_MAIN(None,None,None,True,True,True)

    pathOrg = 'data/organization/'
    pathOrgSpringRank = pathOrg + 'SpringRank/'

    generate_chordDiagram_MAIN(pathOrgSpringRank+'alltypes/allflows/allcountries/','Done chordDiagram')

    # pathOrg = 'data/organization/'
    # pathOrgPer = pathOrg + 'person/'

    # ID_TO_ROR = readCsv('source/ROR_data.csv')
    # ID_RINGGOLD_TO_ISNI = readTsv_ID_RINGGOLD_TO_ISNI('source/aligned_ringgold_and_isni.tsv')

    # xml_files = glob.glob('D:/ORCID_2/*.xml')

    # perData = perData_raw_MAIN(ID_RINGGOLD_TO_ISNI,ID_TO_ROR,xml_files,pathOrgPer+'personal_data_raw.csv',pathOrgPer+'personal_data_raw_flatten.csv','Done perData_raw')

    # perData = readCsv_perData(pathOrgPer+'personal_data_raw_filled.csv')

    # perDataPaired = perData_pair_MAIN(perData,ID_RINGGOLD_TO_ISNI,ID_TO_ROR,pathOrgPer+'personal_data_raw_paired.csv',pathOrgPer+'personal_data_raw_paired_flatten.csv','Done perData_paired',False)

    # perDataPaired = perData_pair_MAIN(perData,ID_RINGGOLD_TO_ISNI,ID_TO_ROR,pathOrgPer+'personal_data_raw_filled.csv',pathOrgPer+'personal_data_raw_filled_flatten.csv','Done perData_filled',True)

    # ROLETITLE_INDEX = readCsv_roleTitle('source/role_title.csv')
    # DEPT_INDEX = readCsv_roleTitle('source/department_name.csv')

    # perDataRoleTitled = perDataRoleTitled_MAIN(perData,ROLETITLE_INDEX,pathOrgPer+'personal_data_roletitled.csv',pathOrgPer+'personal_data_roletitled_flatten.csv','Done perDataRoleTitled')

    # perDataRoleTitled = readCsv_perData(pathOrgPer+'personal_data_roletitled.csv')

    # perDataDepted = perDataDepted_MAIN(perDataRoleTitled,DEPT_INDEX,pathOrgPer+'personal_data_depted.csv',pathOrgPer+'personal_data_depted_flatten.csv','Done perDataDepted')

    # dataFlow = dataFlow_MAIN(perData,'Organization/organization_flow.csv','Done dataOrgFlow')

    # perDataRoleTitled = readCsv_perData(pathOrgPer+'personal_data_roletitled.csv')

    # dataFlowRoleTitled = dataFlowRoleTitled_MAIN(perDataRoleTitled,'education','JP',['Ph.D','Position'],pathOrg+'organization_flow_phd_to_position_education_JP.csv','Done dataOrgFlow')

    # dataFlow = readCsv_asDict(pathOrg+'organization_flow_phd_to_position_education_JP.csv')

    # dataCount = dataCount_MAIN(dataFlow,'Organization/organization_count.csv','Done dataOrgCount')

    # dataSpringRank_MAIN(dataFlow,None,None,ID_TO_ROR,pathOrg+'organization_SpringRank_education.csv','Done dataOrgSpringRank')