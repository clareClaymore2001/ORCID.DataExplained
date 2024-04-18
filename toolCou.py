import os,time,random
import xml.etree.ElementTree as ET
import pandas as pd
import glob
import csv

import multiprocessing as mp
import concurrent.futures
from tqdm import tqdm
from joblib import Parallel, delayed

def flatten(matrix):
    return [item for i in tqdm(matrix) for item in i]

def perData_process(xml_file,ns):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    perDataProcess = []

    for actSummary in root.findall('act:activities-summary',ns):
        for edu in actSummary.findall('act:educations',ns):
            for eduAffiliationGroup in edu.findall('act:affiliation-group',ns):
                for eduSummary in eduAffiliationGroup.findall('edu:education-summary',ns):
                    eduStartDate = 0
                    eduOrgName = eduOrgCity = eduOrgCountry = eduOrgRegion = None

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
                            eduOrgCity = eduAddress.find('com:city',ns).text
                            eduOrgCountry = eduAddress.find('com:country',ns).text

                            for eduOrgRegionElement in eduAddress.findall('com:region',ns):
                                eduOrgRegion = eduOrgRegionElement.text

                    if eduStartDate != 0 and eduOrgName is not None:
                        perDataProcess.append({'StartDate': eduStartDate,
                            'OrgName': eduOrgName, 'OrgCity': eduOrgCity, 'OrgRegion': eduOrgRegion, 'OrgCountry': eduOrgCountry})

        for emp in actSummary.findall('act:employments',ns):
            for empAffiliationGroup in emp.findall('act:affiliation-group',ns):
                for empSummary in empAffiliationGroup.findall('emp:employment-summary',ns):
                    empStartDate = 0
                    empOrgName = empOrgCity = empOrgRegion = empOrgCountry = None

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
                            empOrgCity = empAddress.find('com:city',ns).text
                            empOrgCountry = empAddress.find('com:country',ns).text

                            for empOrgRegionElement in empAddress.findall('com:region',ns):
                                empOrgRegion = empOrgRegionElement.text

                    if empStartDate != 0 and empOrgName is not None:
                        perDataProcess.append({'StartDate': empStartDate,
                            'OrgName': empOrgName, 'OrgCity': empOrgCity, 'OrgRegion': empOrgRegion, 'OrgCountry': empOrgCountry})

    return perDataProcess

def perData_proc_batch(batch,ns):
  return [perData_process(xml_file,ns)
    for xml_file in tqdm(batch)]

def perData_batch_file(array,n_workers):
  file_len = len(array)
  batch_size = round(file_len / n_workers)
  batches = [array[ix : ix + batch_size]
    for ix in tqdm(range(0,file_len,batch_size))]
  return batches

def perData_sortDate(x):
    return x['StartDate']

def dataFlow_process(perDataElement):
    perDataCouName = []
    dataFlowProcess = []
    
    lenPerDataElement = len(perDataElement)
    for x in tqdm(range(lenPerDataElement)):
        perDataElementX = perDataElement[x]

        lenPerDataElementX = len(perDataElement[x])
        for y in range(1,lenPerDataElementX):
            ori = perDataElementX[y-1]
            des = perDataElementX[y]
    
            dataFlowOrigin = ori['OrgCountry']
            dataFlowDestination = des['OrgCountry']
            dataFlowName = dataFlowOrigin + ' -> ' + dataFlowDestination

            if perDataCouName.count(dataFlowName) == 0:
                perDataCouName.append(dataFlowName)
                dataFlowProcess.append(
                    {'Count': 1, 'CouFlow': dataFlowName, 
                    'OriCountry': ori['OrgCountry'], 'DesCountry': des['OrgCountry']})
            else:
                i = perDataCouName.index(dataFlowName)
                dataFlowProcess[i]['Count'] = dataFlowProcess[i]['Count'] + 1

    return dataFlowProcess

def dataFlow_batch_file(array,n_workers):
  file_len = len(array)
  batch_size = round(file_len / n_workers)
  batches = [array[ix : ix + batch_size]
    for ix in tqdm(range(0,file_len,batch_size))]
  return batches

def dataFlow_sortOrgFlow(x):
    return x['CouFlow']

def dataCountOrigin_process(dataFlowElement):
    dataFlowCouName = []
    dataCountProcess = []

    for x in tqdm(dataFlowElement):
        oriCouName = x['OriCountry']
        desCouName = x['DesCountry']
        xCount = x['Count']

        i1 = dataFlowCouName.count(oriCouName) == 0
        i2 = dataFlowCouName.count(desCouName) == 0

        if i1 and i2:
            if oriCouName == desCouName:
                dataFlowCouName.append(oriCouName)
                dataCountProcess.append({'CouName': oriCouName, 'In': 0, 'Out': 0, 'Self': xCount})
            else:
                dataFlowCouName.append(oriCouName)
                dataFlowCouName.append(desCouName)
                dataCountProcess.append({'CouName': oriCouName, 'In': 0, 'Out': xCount, 'Self': 0})
                dataCountProcess.append({'CouName': desCouName, 'In': xCount, 'Out': 0, 'Self': 0})

        elif (i1 or i2) is not True:
            if oriCouName == desCouName:
                dataCountProcess[dataFlowCouName.index(oriCouName)]['Self'] += xCount
            else:
                dataCountProcess[dataFlowCouName.index(desCouName)]['In'] += xCount
                dataCountProcess[dataFlowCouName.index(oriCouName)]['Out'] += xCount

        elif i1:
            dataFlowCouName.append(oriCouName)
            dataCountProcess[dataFlowCouName.index(desCouName)]['In'] += xCount
            dataCountProcess.append({'CouName': oriCouName, 'In': 0, 'Out': xCount, 'Self': 0})
        else:
            dataFlowCouName.append(desCouName)
            dataCountProcess[dataFlowCouName.index(oriCouName)]['Out'] += xCount
            dataCountProcess.append({'CouName': desCouName, 'In': xCount, 'Out': 0, 'Self': 0})

    return dataCountProcess

def dataCountAdjusted_process(dataFlowElement,HDIElement):
    dataFlowCouName = []
    dataCountProcess = []

    for x in tqdm(dataFlowElement):
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
            adjustHDI = h1 / h2
        else:
            adjustHDI = 1

        zCount = xCount * adjustHDI

        i1 = dataFlowCouName.count(oriCouName) == 0
        i2 = dataFlowCouName.count(desCouName) == 0

        if i1 and i2:
            if oriCouName == desCouName:
                dataFlowCouName.append(oriCouName)
                dataCountProcess.append({'CouName': oriCouName, 'In': 0, 'Out': 0, 'Self': zCount})
            else:
                dataFlowCouName.append(oriCouName)
                dataFlowCouName.append(desCouName)
                dataCountProcess.append({'CouName': oriCouName, 'In': 0, 'Out': zCount, 'Self': 0})
                dataCountProcess.append({'CouName': desCouName, 'In': zCount, 'Out': 0, 'Self': 0})

        elif (i1 or i2) is not True:
            if oriCouName == desCouName:
                dataCountProcess[dataFlowCouName.index(oriCouName)]['Self'] += zCount
            else:
                dataCountProcess[dataFlowCouName.index(desCouName)]['In'] += zCount
                dataCountProcess[dataFlowCouName.index(oriCouName)]['Out'] += zCount

        elif i1:
            dataFlowCouName.append(oriCouName)
            dataCountProcess[dataFlowCouName.index(desCouName)]['In'] += zCount
            dataCountProcess.append({'CouName': oriCouName, 'In': 0, 'Out': zCount, 'Self': 0})
        else:
            dataFlowCouName.append(desCouName)
            dataCountProcess[dataFlowCouName.index(oriCouName)]['Out'] += zCount
            dataCountProcess.append({'CouName': desCouName, 'In': zCount, 'Out': 0, 'Self': 0})

    return dataCountProcess

def dataCount_batch_file(array,n_workers):
  file_len = len(array)
  batch_size = round(file_len / n_workers)
  batches = [array[ix : ix + batch_size]
    for ix in tqdm(range(0,file_len,batch_size))]
  return batches

def dataCount_sortDate(x):
    return x['CouName']

if __name__ == '__main__':
    directory_path = "D:/ORCID_2023_10_summaries"
    xml_files = glob.glob(directory_path + "/*/*.xml")

    max_workers = 2 * mp.cpu_count()
    preDataLen = len(xml_files)
    n_workers = preDataLen if max_workers > preDataLen else max_workers

    print(f"{n_workers} workers are available")

    ns = {'com':'http://www.orcid.org/ns/common',
        'act':'http://www.orcid.org/ns/activities',
        'edu':'http://www.orcid.org/ns/education',
        'emp':'http://www.orcid.org/ns/employment'}

    perData = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(perData_proc_batch)(batch,ns)
        for batch in tqdm(perData_batch_file(xml_files,n_workers)))

    perDataFlatten = [x for x in flatten(perData) if x]
    perDataFlattenLen = len(perDataFlatten)
    for i in range(perDataFlattenLen):
        perDataFlatten[i].sort(key = perData_sortDate)
        
    print('Stage 0 cleared')

    with open('HDI.csv') as f:
        HDI = [{k: v 
            for k, v in row.items()}
                for row in csv.DictReader(f, skipinitialspace=True)]

    max_workers = 2 * mp.cpu_count()
    n_workers = perDataFlattenLen if max_workers > perDataFlattenLen else max_workers

    print(f"{n_workers} workers are available")

    dataFlow = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(dataFlow_process)(batch)
        for batch in tqdm(dataFlow_batch_file(perDataFlatten,n_workers)))

    dataFlowFlatten = [x for x in flatten(dataFlow) if x]
    dataFlowFlatten.sort(key = dataFlow_sortOrgFlow)

    x = 1
    lenDataFlow = len(dataFlowFlatten)
    with tqdm(total = lenDataFlow - 1) as pbar:
        while x < lenDataFlow:
            pbar.update(1)
            i = dataFlowFlatten[x]

            if dataFlowFlatten[x-1]['CouFlow'] == i['CouFlow']:
                dataFlowFlatten[x-1]['Count'] += i['Count']
                del dataFlowFlatten[x]
                lenDataFlow -= 1
            else:
                x += 1

    df = pd.DataFrame(dataFlowFlatten)
    output_file = os.path.join(directory_path, "dataCouFlow.csv")
    df.to_csv(output_file,index=False,encoding='utf-8-sig')

    print(df)
    print("Data exported to", output_file)
    print('Stage 1 cleared')

    max_workers = 2 * mp.cpu_count()
    preDataLen = len(dataFlowFlatten)
    n_workers = preDataLen if max_workers > preDataLen else max_workers

    print(f"{n_workers} workers are available")

    dataCount = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(dataCountOrigin_process)(batch)
        for batch in tqdm(dataCount_batch_file(dataFlowFlatten,n_workers)))

    dataCountFlatten = [x for x in flatten(dataCount) if x]
    dataCountFlatten.sort(key = dataCount_sortDate)

    x = 1
    lenDataCount = len(dataCountFlatten)
    with tqdm(total = lenDataCount - 1) as pbar:
        while x < lenDataCount:
            pbar.update(1)
            i = dataCountFlatten[x]

            if dataCountFlatten[x-1]['CouName'] == i['CouName']:
                dataCountFlatten[x-1]['In'] += i['In']
                dataCountFlatten[x-1]['Out'] += i['Out']
                dataCountFlatten[x-1]['Self'] += i['Self']
                del dataCountFlatten[x]
                lenDataCount -= 1
            else:
                x += 1

    df = pd.DataFrame(dataCountFlatten)
    output_file = os.path.join(directory_path, "dataCouCountOrigin.csv")
    df.to_csv(output_file,index=False,encoding='utf-8-sig')

    print(df)
    print("Data exported to", output_file)
    print('Stage 2 cleared')

    max_workers = 2 * mp.cpu_count()
    preDataLen = len(dataFlowFlatten)
    n_workers = preDataLen if max_workers > preDataLen else max_workers

    print(f"{n_workers} workers are available")

    dataCount = Parallel(n_jobs=n_workers,backend="multiprocessing")(delayed(dataCountAdjusted_process)(batch,HDI)
        for batch in tqdm(dataCount_batch_file(dataFlowFlatten,n_workers)))

    dataCountFlatten = [x for x in flatten(dataCount) if x]
    dataCountFlatten.sort(key = dataCount_sortDate)

    x = 1
    lenDataCount = len(dataCountFlatten)
    with tqdm(total = lenDataCount - 1) as pbar:
        while x < lenDataCount:
            pbar.update(1)
            i = dataCountFlatten[x]

            if dataCountFlatten[x-1]['CouName'] == i['CouName']:
                dataCountFlatten[x-1]['In'] += i['In']
                dataCountFlatten[x-1]['Out'] += i['Out']
                dataCountFlatten[x-1]['Self'] += i['Self']
                del dataCountFlatten[x]
                lenDataCount -= 1
            else:
                x += 1

    df = pd.DataFrame(dataCountFlatten)
    output_file = os.path.join(directory_path, "dataCouCountAdjusted.csv")
    df.to_csv(output_file,index=False,encoding='utf-8-sig')

    print(df)
    print("Data exported to", output_file)
    print('Stage 3 cleared')