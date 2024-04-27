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

import math
import matplotlib.pyplot as plt
import matplotlib.scale
from matplotlib.pyplot import figure
from matplotlib.lines import Line2D
from adjustText import adjust_text
import random

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

            if arrayXY['OrgLocationCountry'] in country:
                count0 += 1
                arrayStayRecord.append(arrayXY)

            y += 1

        if count0 > 0:
            count1 = 0

            while count1 == 0 and y < lenArrayX:
                arrayXY = arrayX[y]
                stayRecord = arrayXY['OrgLocationCountry']

                if stayRecord not in country and arrayXY['RoleTitle'] == RT:
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
                        if  stayRecord not in country and arrayXY['RoleTitle'] == RT:
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

def generate_ScatterChart_org_MAIN(df,fileName,cleared):
    # judge = []
    '''for k,row in tqdm(df.iterrows()):
        if judge.count(' '.join([str(row['Total']),'->',str(row['Rate'])])) == 0:
            judge.append(' '.join([str(row['Total']),'->',str(row['Rate'])]))
        else:
            df = df.drop(k)'''

    figure(num=None, figsize=(16, 24), dpi=400, facecolor='w', edgecolor='k')

    plt.xlabel('Sample Number', fontsize=16, weight='bold')
    plt.ylabel('Remaining Rate', fontsize=16, weight='bold')
    plt.title('Rates of Chinese and Indians remaining in the host country after completing master\'s programs', fontsize=16, weight='bold')
    plt.grid(True)

    region_colors = {
        'Asia':'red', 
        'Axis power':'black', 
        'EU/EEA/CH':'royalblue', 
        'Five Eyes':'orange', 
        'Middle East':'olivedrab'}

    for i,j in df.iterrows():plt.scatter(df.Total[i], df.Rate[i], s=400, alpha = 0.25, color=region_colors[j['Region']])

    plt.xscale('log', base=2, subs=[2**x for x in (0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9)])
    plt.yticks([x for x in (0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)])

    textList = [plt.text(x=row['Total'], y=row['Rate']+(random.random()-0.5)/500+(random.random()-0.5)/750+(random.random()-0.5)/1000, s=row['OrgName'], fontsize=4) for k,row in df.iterrows()]

    custom = [Line2D([], [], marker='.', color=i, linestyle='None', markersize=25) for i in region_colors.values()]
    plt.legend(custom, region_colors.keys(), fontsize=15, loc="upper right")

    plt.annotate('Source: https://github.com/clareClaymore2001/ORCID.DataExplained', xy = (0.002, 0.002), xycoords='axes fraction')

    for i in tqdm(range(128)):
        adjust_text(textList,avoid_self=False,explode_radius=0,only_move='y-')
        adjust_text(textList,avoid_self=False,explode_radius=0,only_move='y+')
        adjust_text(textList,avoid_self=False,explode_radius=0,only_move='y+')
        adjust_text(textList,avoid_self=False,explode_radius=0,only_move='y+')

    plt.savefig(fileName+'.png')
    plt.savefig(fileName+'.svg')

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

    # perDataRoleTitled = readCsv_perData(pathOrgPer+'personal_data_roletitled.csv')

    # dataFlowRunRoleTitled = dataFlowRunRoleTitled_MAIN(perDataRoleTitled,['IN','CN'],'Master',pathRun+'organization_flow_CN&IN_any_to_master.csv','Done dataFlowRun')

    # dataFlowRunRoleTitled = readCsv_asDict(pathRun+'organization_flow_IN_bachelor_to_any.csv')

    # dataCountRunOrgRoleTitled = dataCountRunOrgRoleTitled_MAIN(dataFlowRunRoleTitled,pathRun+'organization_count_CN&IN_any_to_master.csv','Done dataCountRunOrg')

    # dataCountRunOrgRoleTitled = readCsv_asDict(pathRun+'organization_count_CN&IN_any_to_master.csv')

    dataCountRunOrgRoleTitled = pd.read_csv(pathRun+'organization_count_CN&IN_any_to_master_1.csv')

    # dataCountRunCityRoleTitled = dataCountRunCityRoleTitled_MAIN(dataCountRunOrgRoleTitled,pathRun+'city_count_CN&IN_any_to_master.csv','Done dataCountRunCity')

    # dataCountRunCouRoleTitled = dataCountRunCouRoleTitled_MAIN(dataCountRunOrgRoleTitled,pathRun+'country_count_CN&IN_any_to_master.csv','Done dataCountRunCou')

    generate_ScatterChart_org_MAIN(dataCountRunOrgRoleTitled,pathRun+'organization_count_CN&IN_any_to_master_1','Done ScatterChart')