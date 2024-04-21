# ORCID.DataExplained

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
