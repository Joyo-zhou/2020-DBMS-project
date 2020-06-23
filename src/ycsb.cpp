#include <iostream>
#include <string>
#include <io.h>
#include <vector>
#include <fstream>
#include <sstream>
#include <ctime>

#include "gtest/gtest.h"
#include "../include/pm_ehash.h"

using namespace std;

//拿到workloads文件夹下所有子文件的路径信息（可以不用管）
void getFiles(const std::string &path, std::vector<std::string> &files)
{
    //文件句柄
    long long hFile = 0;
    //文件信息，_finddata_t需要io.h头文件
    struct _finddata_t fileinfo;
    std::string p;
    int i = 0;
    if ((hFile = _findfirst(p.assign(path).append("\\*").c_str(), &fileinfo)) != -1)
    {
        do
        {
            files.push_back(p.assign(path).append("\\").append(fileinfo.name));
        } while (_findnext(hFile, &fileinfo) == 0);
        _findclose(hFile);
    }
}

int main()
{
    string path = "../workloads";
    vector<string> loadsPath;
    getFiles(path, loadsPath); // loadsPaths中呈装workloads中所有文本文件的相对地址（可以直接使用）
    vector<string> file;       // 文本文件内容的容器

    for (int i = 0; i < loadsPath.size(); ++i)      // 遍历workloads文件夹下所有txt文件
    {
        file.clear();

        ifstream inFile;
        inFile.open(loadsPath[i]);
        string tmp;
        while (getline(inFile, tmp))
        {
            file.push_back(tmp);
        } // 将读取的命令按行存储在file中

        PmEHash *ehash = new PmEHash;           // 创建数据表，开始测试！

        int insertCount = 0;
        int readCount = 0;                      // 记录操作的次数

        clock_t startTime, endTime;             // 用来计算操作使用的时间
        clock_t totalTime;                      // 记录执行所有操作的总时间

        for (int j = 0; j < file.size(); ++j)
        { // 遍历命令，进行数据库操作，记录参数
            istringstream inS(file[j]);
            string order;
            int number;
            inS >> order >> number;

            if (order == "INSERT")
            {
                startTime=clock();                          // 计时开始

                insertCount++;
                kv temp;
                temp.key = temp.value = number;
                ehash->insert(temp);

                endTime=clock();                            // 计时结束
                totalTime+=(double)(endTime-startTime);     // 记录用时 
            }
            else if (order == "READ")
            {
                startTime=clock();                          // 计时开始

                readCount++;
                ehash->search(number);
                
                endTime=clock();                            // 计时结束
                totalTime+=(double)(endTime-startTime);     // 记录用时 
            }
        }

        // 输出性能测试内容：
        cout<<"insert read command  time "<<endl;
        cout<<insertCount<<" "<<readCount<<" "<<file.size()<<" "<<totalTime<<endl;

        ehash->selfDestory(); // 回收数据表
    }
}