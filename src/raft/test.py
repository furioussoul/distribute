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


img_dir = "/Users/szj/Downloads/关键点数据"
data_path = "/Users/szj/Downloads/train/data_info.json"

def deal_data(context):
    file = open(data_path)
    for line in file:
        deal_line2(context,line)

def deal_line2(context,line):
    item = line.split(" ")
    image_path = item[1]
    im_split = image_path.split("/")
    image_name = im_split[len(im_split)-1]
    context[image_name]['label'] = item

def deal_images(context):
    dirs = os.listdir(img_dir)
    for file in dirs:
        if ".png" not in file :
            continue
        img = Image.open(img_dir + "/" + file)
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
                xy['axisY'] = str(float(float(coordinates[i])*val['height']))
                list.append(xy)

        label_file_path = (img_dir+"/"+key ).replace("png","txt")

        label_file = open(label_file_path, 'w')
        label_file.writelines(json.dumps([{
            "coordinates": list
        }]))

context = {}
deal_images(context)
deal_data(context)
convert_to_label_result(context)


# file = open(path)
#
# for line in file:
#     deal_line2(line)
#
# file.close()
