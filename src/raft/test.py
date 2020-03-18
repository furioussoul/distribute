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
#
# data1 = {'release_center':'/gpfs/application/train/f0480ef3c622486ea2e1df4c4d719815/model/release','gpus':0}
#
# json.dumps(data1)


from PIL import Image
import pylab as pl
import numpy as np

ar1 = [0.101363 ,0.308037 ,0.107211 ,0.342544 ,0.115 ,0.393206 ,0.128237 ,0.435205 ,0.135282 ,0.480131 ,0.139852 ,0.518742 ,0.169908 ,0.555984 ,0.184949 ,0.592566 ,0.228004 ,0.651342 ,0.235022 ,0.686075 ,0.257238 ,0.729555 ,0.279116 ,0.748378 ,0.310022 ,0.778409 ,0.327087 ,0.799825 ,0.364935 ,0.836851 ,0.395236 ,0.856894 ,0.400187 ,0.875212 ,0.478448 ,0.839413 ,0.523892 ,0.851436 ,0.578204 ,0.842974 ,0.654887 ,0.783827 ,0.688292 ,0.771591 ,0.742094 ,0.713013 ,0.774637 ,0.679861 ,0.792976 ,0.627146 ,0.823687 ,0.573527 ,0.836657 ,0.531653 ,0.859283 ,0.460179 ,0.869142 ,0.393346 ,0.856661 ,0.366169 ,0.844691 ,0.3051 ,0.860102 ,0.235154 ,0.822896 ,0.179409 ,0.0605909 ,0.214589 ,0.0904323 ,0.147873 ,0.123046 ,0.145793 ,0.137665 ,0.134099 ,0.19881 ,0.128424 ,0.195888 ,0.161505 ,0.172036 ,0.171575 ,0.12171 ,0.185935 ,0.0922764 ,0.201378 ,0.335175 ,0.111633 ,0.383634 ,0.0932157 ,0.463406 ,0.0552418 ,0.521416 ,0.0426587 ,0.584409 ,0.0817738 ,0.526238 ,0.126442 ,0.47329 ,0.118094 ,0.395555 ,0.120187 ,0.346543 ,0.132816 ,0.261854 ,0.234915 ,0.252259 ,0.25906 ,0.256517 ,0.328039 ,0.24459 ,0.395703 ,0.216504 ,0.465641 ,0.278206 ,0.466644 ,0.324636 ,0.455493 ,0.348124 ,0.421417 ,0.407778 ,0.409663 ,0.114985 ,0.270805 ,0.100342 ,0.250096 ,0.154695 ,0.233527 ,0.177605 ,0.235108 ,0.21764 ,0.253158 ,0.175682 ,0.25975 ,0.166237 ,0.281898 ,0.136041 ,0.274339 ,0.398529 ,0.208632 ,0.433196 ,0.178806 ,0.480483 ,0.146543 ,0.498565 ,0.159239 ,0.526432 ,0.195368 ,0.504884 ,0.202151 ,0.448679 ,0.233491 ,0.434364 ,0.214773 ,0.243955 ,0.610871 ,0.25453 ,0.548081 ,0.305737 ,0.540797 ,0.293951 ,0.521992 ,0.34628 ,0.525017 ,0.438376 ,0.523942 ,0.515406 ,0.544331 ,0.485424 ,0.614755 ,0.437375 ,0.653134 ,0.362284 ,0.647013 ,0.318874 ,0.658607 ,0.262045 ,0.620542 ,0.259545 ,0.616444 ,0.296066 ,0.577621 ,0.354625 ,0.564245 ,0.424099 ,0.528903 ,0.495252 ,0.564385 ,0.458684 ,0.586751 ,0.334901 ,0.63786 ,0.314271 ,0.620879 ,0.15993 ,0.254235 ,0.453646 ,0.156499 ,1.61536e-15 ,0 ]
ar2 = [0.111362 ,0.218903 ,0.119921 ,0.308954 ,0.115038 ,0.335423 ,0.129664 ,0.392225 ,0.107684 ,0.445851 ,0.13394 ,0.481355 ,0.147257 ,0.540681 ,0.184732 ,0.59009 ,0.206524 ,0.662066 ,0.226369 ,0.725291 ,0.248584 ,0.751993 ,0.298324 ,0.770999 ,0.329 ,0.84135 ,0.41559 ,0.859133 ,0.465963 ,0.877916 ,0.482582 ,0.888165 ,0.520255 ,0.888967 ,0.609707 ,0.858096 ,0.62428 ,0.844951 ,0.675737 ,0.815233 ,0.713712 ,0.79611 ,0.760876 ,0.740532 ,0.774171 ,0.713495 ,0.796316 ,0.660279 ,0.823933 ,0.62235 ,0.827494 ,0.566752 ,0.865638 ,0.524132 ,0.87702 ,0.466853 ,0.880709 ,0.407462 ,0.868272 ,0.355307 ,0.877196 ,0.288655 ,0.881097 ,0.249168 ,0.85295 ,0.175041 ,0.197473 ,0.225177 ,0.27675 ,0.159273 ,0.336968 ,0.141067 ,0.380503 ,0.145841 ,0.434808 ,0.16582 ,0.437228 ,0.20168 ,0.403309 ,0.197836 ,0.333006 ,0.179618 ,0.289704 ,0.196582 ,0.563192 ,0.169534 ,0.613033 ,0.148111 ,0.669698 ,0.131293 ,0.729627 ,0.122438 ,0.795595 ,0.194137 ,0.745741 ,0.173867 ,0.668698 ,0.181699 ,0.619448 ,0.181827 ,0.56364 ,0.217992 ,0.497264 ,0.282603]

img1 = '/workspace/data/test_data/16_35_Basketball_basketballgame_ball_35_341_0.png'
img2 = "/workspace/data/test_data/28_33_Running_Running_33_526_0.png"

x = ar1[::2]
y = ar1[1::2]

# 读取图像到数组中
im = pl.array(Image.open(img2))
width = im.shape[0]
height = im.shape[1]

x = np.array(x) * width
y = np.array(y) * height

# 绘制图像
pl.imshow(im)
# 一些点

# 使用红色星状标记绘制点
pl.plot(x,y,'b*')
pl.show()


