# -*- coding: utf-8 -*-

import json
import os

from PIL import Image


def deal_line(line):
    line = line.replace("/home/yangjunqi/pytorch-landmark/PFLD-pytorch/data", "/Users/szj/Downloads")
    img_path = line.split(" ")[0]
    line_split = line.split(" ")
    axises = line_split[1:len(line_split) - 9]
    img = Image.open(img_path)

    coordinates = []

    i = 1
    xy = {
        "classification":1
    }
    for axis in axises:
        if i % 2 != 0:
            xy['axisX'] = str(int(float(axis) * img.width))
        else:
            xy['axisY'] = str(int(float(axis) * img.height))
            coordinates.append(xy)
            xy = {
                "classification": 1
            }
        i += 1

    label_file_path = img_path.replace("png", "txt")

    label_file = open(label_file_path, 'w')
    label_file.writelines(json.dumps([{"classification": {"性别": 0}}, {
        "coordinates": coordinates
    }]))




def deal_data(context):
    file = open(context['data_path'])
    for line in file:
        deal_line2(context,line)

def deal_line2(context,line):
    item = line.split(" ")
    image_path = item[1]
    im_split = image_path.split("/")
    image_name = im_split[len(im_split)-1]
    context[image_name]['label'] = item

def deal_images(context):
    dirs = os.listdir(context['img_dir'])
    for file in dirs:
        if ".png" not in file :
            continue
        img = Image.open(context['img_dir'] + "/" + file)
        if file not in context:
            context[file] = {}
        context[file]['width'] = img.width
        context[file]['height'] = img.height

def convert_to_label_result(context):

    for key in context:
        list = []

        val = context[key]
        coordinates = val['label'][2:]
        xy = {}
        for i in range(len(coordinates)):

            if i % 2 == 0:
                xy = {}
                xy['axisX'] = str(float(float(coordinates[i])*val['width']))
            else:
                xy['axisY'] = str(float(float(coordinates[i]) * val['height']))
                list.append(xy)

        label_file_path = (context['img_dir'] + "/" + key).replace("png", "txt")

        label_file = open(label_file_path, 'w')
        label_file.writelines(json.dumps([{
            "coordinates": list
        }]))


def convertKeyPoint():
    img_dir = "/Users/szj/Downloads/关键点数据"
    data_path = "/Users/szj/Downloads/train/data_info.json"
    context = {}
    context['img_dir'] = img_dir
    context['data_path'] = data_path
    deal_images(context)
    deal_data(context)
    convert_to_label_result(context)


def convertCifar10():
    img_dir = "/Users/szj/Downloads/class"
    data_path = "/Users/szj/Downloads/train.txt"
    context = {}
    context['img_dir'] = img_dir
    context['data_path'] = data_path
    deal_cifar10_img(context)
    file = open(context['data_path'])
    for line in file:
        deal_cifar10_data(context, line)


def deal_cifar10_img(context):
    dirs = os.listdir(context['img_dir'])
    for file in dirs:
        if file not in context:
            context[file] = {}


def deal_cifar10_data(context, line):
    item = line.split(" ")
    image_path = item[0]
    im_split = image_path.split("/")
    image_name = im_split[len(im_split) - 1]
    if image_name in context:
        label = int(item[3].replace("\n", ""))
        label_file_path = (context['img_dir'] + "/" + image_name).replace("jpg", "txt")
        label_file = open(label_file_path, 'w')
        label_file.writelines(json.dumps([{
            "classification": {"种类" :label}
        }],ensure_ascii=False))
        print(label)


# convertCifar10()
# file = open(path)
#
# for line in file:
#     deal_line2(line)
#
# file.close()

data1 = {'release_center':'/gpfs/application/train/f0480ef3c622486ea2e1df4c4d719815/model/release','gpus':0}

json.dumps(data1)

