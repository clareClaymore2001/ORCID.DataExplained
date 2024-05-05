# ORCID.DataExplained

原帖：

[学术圈都在往哪润？来自 1847 万名学者 1145 万次的用脚投票](https://www.reddit.com/r/iwanttorun/comments/1c7o1a3/%E5%AD%A6%E6%9C%AF%E5%9C%88%E9%83%BD%E5%9C%A8%E5%BE%80%E5%93%AA%E6%B6%A6%E6%9D%A5%E8%87%AA_1847_%E4%B8%87%E5%90%8D%E5%AD%A6%E8%80%85_1145_%E4%B8%87%E6%AC%A1%E7%9A%84%E7%94%A8%E8%84%9A%E6%8A%95%E7%A5%A8)

[终极赛博斗兽棋之终结一切院校排名的院校排名](https://www.reddit.com/r/iwanttorun/comments/1ccknp6/%E7%BB%88%E6%9E%81%E8%B5%9B%E5%8D%9A%E6%96%97%E5%85%BD%E6%A3%8B%E4%B9%8B%E7%BB%88%E7%BB%93%E4%B8%80%E5%88%87%E9%99%A2%E6%A0%A1%E6%8E%92%E5%90%8D%E7%9A%84%E9%99%A2%E6%A0%A1%E6%8E%92%E5%90%8D)

[盘点老中老印最好留的硕士项目（附国家与地区层级数据）](https://www.reddit.com/r/iwanttorun/comments/1cechcf/%E7%9B%98%E7%82%B9%E8%80%81%E4%B8%AD%E8%80%81%E5%8D%B0%E6%9C%80%E5%A5%BD%E7%95%99%E7%9A%84%E7%A1%95%E5%A3%AB%E9%A1%B9%E7%9B%AE%E9%99%84%E5%9B%BD%E5%AE%B6%E4%B8%8E%E5%9C%B0%E5%8C%BA%E5%B1%82%E7%BA%A7%E6%95%B0%E6%8D%AE)

This is to analyze the annual released ORCID Public Data File

Sorry I chose thousandths place as scale. I would like to regenerate the graph later.

**Here is the explain:**

* When the value comes >1,000, it means the country has a net inflow of scholars, meaning the country intakes more scholars than it loses
* When the value comes <1,000, it means the net outflow of scholars >0%, meaning the country intakes less scholars than it loses
* When the value comes =1,000, it means the country stands at a balance of the scholars intake and lose

**For the scholars:**

* A scholar is a person who has registered on ORCID (whose database I have used in these graphs), which is a website for academic authors and contributers. More info on [Wikipedia](https://en.wikipedia.org/wiki/ORCID)
* The scholars' data I used is based on ORCID Public Data File, which has 11 million flows from 18 million scholars

**For every country:**

* Ratio counted without self flow = Total inflow count / Total outflow count
* Ratio counted with self flow = (Total inflow count + Total self flow count) / (Total outflow count + Total self flow count)

**For every flow:**

* An inflow means a scholar chose the inflow country as the destination country
* An outflow means a scholar made the flow from the outflow country as the origin country
* An self flow means the inflow country is the same as the outflow country, internal flow in other words

**For every flow count:**

* A Count without HDI adjusted = A scholar move once from a position to a position
* A HDI adjusted count = A Count without HDI adjusted \* (Outflow country HDI / Inflow country HDI)

HDI adjusted count can make a flow having a higher or lower count with high HDI difference, which can eliminate attraction from the difference of living condition to some extent.

**GitHub Page:** [ORCID.DataExplained](https://github.com/clareClaymore2001/ORCID.DataExplained)

Where you can download all the data the graph based on.

**Source:**

* [ORCID Public Data File 2023](https://orcid.figshare.com/articles/dataset/ORCID_Public_Data_File_2023/24204912)
* [UNDP HDI 2022](https://hdr.undp.org/sites/default/files/2023-24_HDR/HDR23-24_Statistical_Annex_HDI_Table.xlsx)
* [HDI for Taiwan and Macau](https://en.wikipedia.org/wiki/List_of_administrative_divisions_of_Greater_China_by_Human_Development_Index)

**Tool:**

* [Program by Python, released on GitHub](https://github.com/clareClaymore2001/ORCID.DataExplained)
* [IMAGE](https://gisco-services.ec.europa.eu/image/screen/home)
* [Watermark tool](https://makewatermark.com)

HDI 加权算法：流动数 = 流出国HDI / 流入国HDI

如此一来当学者从 HDI 高的国家流动到 HDI 低的国家时能有更高的加成，反之亦反，以此可以更清晰地划分层次

例如：同样是流入，但是挪威的流入多为发达国家，即 HDI 高的国家的流入，而法德则多为发展中国家，即 HDI 低的国家的流入，所以在加权后挪威的指标可以明显和法德拉开差距
