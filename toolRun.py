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

def dataFlowRunRoleTitled_process(array,country,RT):
    arrayName = []
    arrayProcess = []

    for arrayX in tqdm(array):
        arrayStayRecord = []
        lenArrayX = len(arrayX)

        count0 = 0
        y = 0

        while count0 == 0 and y < lenArrayX:
            arrayXY = arrayX[y]

            if arrayXY['OrgLocationCountry'] == country:
                count0 += 1
                arrayStayRecord.append(arrayXY)

            y += 1

        if count0 > 0:
            count1 = 0

            while count1 == 0 and y < lenArrayX:
                arrayXY = arrayX[y]
                stayRecord = arrayXY['OrgLocationCountry']

                if stayRecord != country and arrayXY['RoleTitle'] == RT:
                    count1 += 1
                    arrayStayRecord.append(arrayXY)

                y += 1

            if count1 > 0:
                if y < lenArrayX:
                    stay = True
                else:
                    stay = None

                while y < lenArrayX:
                    arrayXY = arrayX[y]
                    stayRecord = arrayXY['OrgLocationCountry']

                    if stayRecord != arrayStayRecord[len(arrayStayRecord)-1]['OrgLocationCountry']:
                        if  stayRecord != country and arrayXY['RoleTitle'] == RT:
                            arrayStayRecord.append(arrayXY)
                            count1 += 1

                            if y < lenArrayX - 1:
                                stay = True
                            else:
                                stay = None

                        else:
                            stay = False

                    y += 1

                for i in range(1,count1):
                    des = arrayStayRecord[i]

                    dataFlowDestination = des['DisOrgID']
                    dataFlowJudge = ' '.join([dataFlowDestination,'FALSE'])

                    if arrayName.count(dataFlowJudge) == 0:
                        arrayName.append(dataFlowJudge)
                        arrayProcess.append(
                            {'Count': 1, 'Stay': 'FALSE', 'DisOrgID': dataFlowDestination, 
                            'OrgLocationCountry': des['OrgLocationCountry'], 'OrgLocationDetails': des['OrgLocationDetails'], 
                            'OrgName': des['OrgName'], 'OrgType': des['OrgType']})
                    else:
                        arrayProcess[arrayName.index(dataFlowJudge)]['Count'] += 1

                if stay is not None:
                    des = arrayStayRecord[len(arrayStayRecord)-1]

                    dataFlowDestination = des['DisOrgID']
                    dataFlowJudge = ' '.join([dataFlowDestination,str(stay)])

                    if arrayName.count(dataFlowJudge) == 0:
                        arrayName.append(dataFlowJudge)
                        arrayProcess.append(
                            {'Count': 1, 'Stay': str(stay), 'DisOrgID': dataFlowDestination, 
                            'OrgLocationCountry': des['OrgLocationCountry'], 'OrgLocationDetails': des['OrgLocationDetails'], 
                            'OrgName': des['OrgName'], 'OrgType': des['OrgType']})
                    else:
                        arrayProcess[arrayName.index(dataFlowJudge)]['Count'] += 1

    return arrayProcess

def dataFlowRunRoleTitled_sortDisOrgID(array):
    return array['DisOrgID']

def dataFlowRunRoleTitled_MAIN(perDataRoleTitledElement,country,RT,fileName,cleared):
    n_workers = nWorkers(perDataRoleTitledElement)

    dataFlowFlatten = dataFlowRunRoleTitled_process(perDataRoleTitledElement,country,RT,)

    # dataFlow = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(dataFlowRunRoleTitled_process)(batch)
    #     for batch in tqdm(batch_file(perDataRoleTitledElement,n_workers)))

    # dataFlowFlatten = [x for x in flatten(dataFlow,1) if x]

    dataFlowFlatten.sort(key = dataFlowRunRoleTitled_sortDisOrgID)

    x = 1
    lenDataFlow = len(dataFlowFlatten)
    with tqdm(total = lenDataFlow - 1) as pbar:
        while x < lenDataFlow:
            pbar.update(1)
            i1 = dataFlowFlatten[x-1]
            i2 = dataFlowFlatten[x]

            if i1['DisOrgID'] == i2['DisOrgID'] and i1['Stay'] == i2['Stay']:
                i1['Count'] += i2['Count']
                del dataFlowFlatten[x]
                lenDataFlow -= 1
            else:
                x += 1

    exportFile(dataFlowFlatten,fileName)

    print(cleared)

    return dataFlowFlatten

def dataCountRunOrgRoleTitled_process(dataFlowElement):
    dataFlowOrgID = []
    dataCountProcess = []

    for x in tqdm(dataFlowElement):
        OrgID = x['DisOrgID']
        xCount = x['Count']

        if str(x['Stay']) == 'True':xStay = 'Stay'
        else:xStay = 'Leave'

        i = dataFlowOrgID.count(OrgID) == 0

        if i:
            dataFlowOrgID.append(OrgID)

            if xStay == 'Stay':
                dataCountProcess.append({'OrgID': OrgID, 
                    'OrgLocationCountry': x['OrgLocationCountry'], 'OrgLocationDetails': x['OrgLocationDetails'], 
                    'OrgName': x['OrgName'], 'OrgType': x['OrgType'], 'Stay': xCount, 'Leave': 0})
            else:
                dataCountProcess.append({'OrgID': OrgID, 
                    'OrgLocationCountry': x['OrgLocationCountry'], 'OrgLocationDetails': x['OrgLocationDetails'], 
                    'OrgName': x['OrgName'], 'OrgType': x['OrgType'], 'Stay': 0, 'Leave': xCount})

        else:
            dataCountProcess[dataFlowOrgID.index(OrgID)][xStay] += xCount
            dataCountProcess[dataFlowOrgID.index(OrgID)][xStay] += xCount

    return dataCountProcess

def dataCountRunOrgRoleTitled_sortStay(array):
    return array['Stay']

def dataCountRunOrgRoleTitled_sortStayRate(array):
    return (array['Leave'] / (array['Stay'] + array['Leave']))

def dataCountRunOrgRoleTitled_MAIN(dataFlowRunRoleTitledElement,fileName,cleared):
    n_workers = nWorkers(dataFlowRunRoleTitledElement)

    dataCountFlatten = dataCountRunOrgRoleTitled_process(dataFlowRunRoleTitledElement)

    # dataCount = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(dataCountRunOrgRoleTitled_process)(batch)
    #     for batch in tqdm(batch_file(dataFlowRunRoleTitledElement,n_workers)))

    # dataCountFlatten = [x for x in flatten(dataCount,1) if x]

    dataCountFlatten.sort(key = dataCountRunOrgRoleTitled_sortStay,reverse=True)
    dataCountFlatten.sort(key = dataCountRunOrgRoleTitled_sortStayRate)

    x = 1
    lenDataCount = len(dataCountFlatten)
    with tqdm(total = lenDataCount - 1) as pbar:
        while x < lenDataCount:
            pbar.update(1)
            i = dataCountFlatten[x]

            if dataCountFlatten[x-1]['OrgID'] == i['OrgID']:
                dataCountFlatten[x-1]['Stay'] += i['Stay']
                dataCountFlatten[x-1]['Leave'] += i['Leave']
                del dataCountFlatten[x]
                lenDataCount -= 1
            else:
                x += 1

    exportFile(dataCountFlatten,fileName)

    print(cleared)

def dataCountRunCityRoleTitled_process(array):
    arrayName = []
    arrayProcess = []

    for item in array:
        itemCou = item['OrgLocationCountry']
        itemCity = item['OrgLocationDetails']

        if arrayName.count(' '.join([itemCou,'->',itemCity])) == 0:
            arrayName.append(' '.join([itemCou,'->',itemCity]))
            arrayProcess.append({'Country': itemCou, 'City': itemCity, 'Stay': item['Stay'], 'Leave': item['Leave']})
        else:
            i = arrayName.index(' '.join([itemCou,'->',itemCity]))
            arrayProcess[i]['Stay'] += item['Stay']
            arrayProcess[i]['Leave'] += item['Leave']

    return arrayProcess

def dataCountRunCityRoleTitled_MAIN(dataCountRunOrgRoleTitledElement,fileName,cleared):
    n_workers = nWorkers(dataCountRunOrgRoleTitledElement)

    dataCountFlatten = dataCountRunCityRoleTitled_process(dataCountRunOrgRoleTitledElement)

    dataCountFlatten.sort(key = dataCountRunOrgRoleTitled_sortStay,reverse=True)
    dataCountFlatten.sort(key = dataCountRunOrgRoleTitled_sortStayRate)

    x = 1
    lenDataCount = len(dataCountFlatten)
    with tqdm(total = lenDataCount - 1) as pbar:
        while x < lenDataCount:
            pbar.update(1)
            i = dataCountFlatten[x]

            if dataCountFlatten[x-1]['Country'] == i['Country'] and dataCountFlatten[x-1]['City'] == i['City']:
                dataCountFlatten[x-1]['Stay'] += i['Stay']
                dataCountFlatten[x-1]['Leave'] += i['Leave']
                del dataCountFlatten[x]
                lenDataCount -= 1
            else:
                x += 1

    exportFile(dataCountFlatten,fileName)

    print(cleared)

def dataCountRunCouRoleTitled_process(array):
    arrayName = []
    arrayProcess = []

    for item in array:
        itemCou = item['OrgLocationCountry']

        if arrayName.count(itemCou) == 0:
            arrayName.append(itemCou)
            arrayProcess.append({'Country': itemCou, 'Stay': item['Stay'], 'Leave': item['Leave']})
        else:
            i = arrayName.index(itemCou)
            arrayProcess[i]['Stay'] += item['Stay']
            arrayProcess[i]['Leave'] += item['Leave']

    return arrayProcess

def dataCountRunCouRoleTitled_MAIN(dataCountRunOrgRoleTitledElement,fileName,cleared):
    n_workers = nWorkers(dataCountRunOrgRoleTitledElement)

    dataCountFlatten = dataCountRunCouRoleTitled_process(dataCountRunOrgRoleTitledElement)

    dataCountFlatten.sort(key = dataCountRunOrgRoleTitled_sortStay,reverse=True)
    dataCountFlatten.sort(key = dataCountRunOrgRoleTitled_sortStayRate)

    x = 1
    lenDataCount = len(dataCountFlatten)
    with tqdm(total = lenDataCount - 1) as pbar:
        while x < lenDataCount:
            pbar.update(1)
            i = dataCountFlatten[x]

            if dataCountFlatten[x-1]['Country'] == i['Country']:
                dataCountFlatten[x-1]['Stay'] += i['Stay']
                dataCountFlatten[x-1]['Leave'] += i['Leave']
                del dataCountFlatten[x]
                lenDataCount -= 1
            else:
                x += 1

    exportFile(dataCountFlatten,fileName)

    print(cleared)

if __name__ == '__main__':
    pathRun = 'data/run/'
    pathOrg = 'data/organization/'
    pathOrgPer = pathOrg + 'person/'

    # ID_TO_ROR = readCsv('source/ROR_data.csv')
    # ID_RINGGOLD_TO_ISNI = readTsv_ID_RINGGOLD_TO_ISNI('source/aligned_ringgold_and_isni.tsv')

    # xml_files = glob.glob('D:/ORCID_2/*.xml')

    # ROLETITLE_INDEX = readCsv_roleTitle('source/role_title.csv')
    # DEPT_INDEX = readCsv_roleTitle('source/department_name.csv')

    # perDataDepted = perDataDepted_MAIN(perDataRoleTitled,DEPT_INDEX,pathOrgPer+'personal_data_depted_EandS.csv',pathOrgPer+'personal_data_depted_EandS_flatten.csv','Done perDataDepted')

    # dataFlow = dataFlow_MAIN(perData,'Organization/organization_flow.csv','Done dataOrgFlow')

    # perDataRoleTitled = readCsv_perData(pathOrgPer+'personal_data_roletitled.csv')

    # dataFlowRunRoleTitled = dataFlowRunRoleTitled_MAIN(perDataRoleTitled,'IN','Master',pathRun+'organization_flow_IN_any_to_master.csv','Done dataFlowRun')

    # dataFlowRunRoleTitled = readCsv_asDict(pathRun+'organization_flow_IN_bachelor_to_any.csv')

    # dataCountRunOrgRoleTitled = dataCountRunOrgRoleTitled_MAIN(dataFlowRunRoleTitled,pathRun+'organization_count_IN_any_to_master.csv','Done dataCountRunOrg')

    dataCountRunOrgRoleTitled = readCsv_asDict(pathRun+'organization_count_IN_any_to_master.csv')

    dataCountRunCityRoleTitled = dataCountRunCityRoleTitled_MAIN(dataCountRunOrgRoleTitled,pathRun+'city_count_IN_any_to_master.csv','Done dataCountRunCity')

    # dataCountRunCouRoleTitled = dataCountRunCouRoleTitled_MAIN(dataCountRunOrgRoleTitled,pathRun+'country_count_CN_any_to_master.csv','Done dataCountRunCou')

    # dataFlow = readCsv_asDict(pathOrg+'organization_flow_phd_to_position_education_JP.csv')

    # dataCount = dataCount_MAIN(dataFlow,'Organization/organization_count.csv','Done dataOrgCount')