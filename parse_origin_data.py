"""
原始数据中，会存在一些换行符，'\n'，所以，如果以csv格式获取数据会出现解析问题，一个替代的选择就是json
原始数据存为json，然后手动除去非法字符，如`\u202d`，`\n`等
"""


import pandas as pd
import time
from datetime import datetime
import os
import json
import pickle

def load_json(data_dir) :
    """
    载入原始Json数据
    """
    with open(data_dir+"question.json", "r", encoding="utf-8") as fi :
        question = json.load(fi)
    print("the info about orogin json data:")
    check_json(question)

    question = question["RECORDS"]
    return question

def check_json(question) :
    """
    输出一些原始json数据的基本信息
    """
    print("type of origin data:", type(question))
    record = question["RECORDS"]
    print("type of records:", type(record))
    print("length of records:", len(record))



def generate_question_dict(question) :
    """
    根据原始json数据，生成一个question字典
    """
    question_dict = {"qid":[], "tags_str":[], "create_time":[], "tags":[]}
    for index, item in enumerate(question) :
        # print("\r", index, end="")
        result = parse(item)
        if result and len(result[-1])>1 :
            question_dict["qid"].append(result[0])
            question_dict["tags_str"].append(result[1])
            question_dict["create_time"].append(result[2])
            question_dict["tags"].append(result[3])
    
    print("info about question dict:")
    for key in question_dict.keys() :
        print(key, len(question_dict[key])) 

    return question_dict


def parse(item) :
    """
    从每个条目里面取出需要的问题的字段
    """
    qid = int(item["qid"])
    topics = item["topics"].replace("\n", "").replace("\u202d", "")
    tags = topics.split(",")

    if item["create_time"]=="" :
        return 0
    else :
        create_time = int(item["create_time"])

    return [qid, topics, create_time, tags]


def translate_tag2num(question_dict) :
    """
    根据已经解析出来的问题的字典 解析和提取标签，完成标签的数字化任务
    生成tag_list和tag_dict
    """
    all_tag_dict = {}
    tag_list = []
    current_tag_index = 0
    for index, tags in enumerate(question_dict["tags"]) :
        a = []
        for ta in tags :
            if ta not in all_tag_dict :
                all_tag_dict[ta] = [current_tag_index, 1]
                current_tag_index += 1
            else :
                all_tag_dict[ta][1] += 1

            a.append(all_tag_dict[ta][0])

        tag_list.append(a)

    print("there are", len(all_tag_dict.keys()), "different tags")
    print("the index of last tag is:", current_tag_index)

    return all_tag_dict, tag_list


def parse_tag(tag) :
    tag_list = tag.split(";")
    tag_num = len(tag_list)
    return pd.Series({"tag_list":tag_list, "tag_num":tag_num})


def test_dataframe(question_dict4dataframe) :
    table = pd.DataFrame(question_dict4dataframe)
    print(table.head())
    print(table.shape)
    print(table.columns)
    print(table.dtypes)
    print(type(table.loc[0, "tag_list"]))


def get_tag_list(tag_info) :
    """
    根据tag_dict生成一份tag_list
    """
    length = len(tag_info.keys())
    print("there are %s tags"%length)
    tag_list = list(range(length))
    for key in tag_info.keys() :
        tag_list[tag_info[key][0]] = key

    return tag_list

def save_data(question_dict, tag_dict, data_dir) :
    """
    保存question_dict, tag_dict, tag_list
    """
    with open(data_dir+"tag_dict.pkl", "wb") as fi :
        pickle.dump(tag_dict, fi)

    with open(data_dir+"question_dict.pkl", "wb") as fi :
        pickle.dump(question_dict, fi)

    # with open(data_dir+"tag_list.pkl", "wb") as fi :
    #   pickle.dump(tag_list, fi)

if __name__ == '__main__':

    origin_data_dir = "../origin_data/"
    data_dir = "data/"

    # try :
    #   os.mkdir(data_dir)
    # except :
    #   pass

    question = load_json(origin_data_dir)

    # check_json(question)

    question_dict = generate_question_dict(question)

    tag_dict, tag_by_index = translate_tag2num(question_dict)

    print(len(tag_by_index))

    question_dict["tag_list"] = tag_by_index


    # tag_list = get_tag_list(tag_dict)

    save_data(question_dict, tag_dict, data_dir)