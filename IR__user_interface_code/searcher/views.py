# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from searcher.forms import TextForm
from django.shortcuts import render
from django.http import HttpResponse
from django.template import Context, loader

#import paramiko
import os 

# Create your views here.
def index(request):
    return render( request, 'index.html')
	    

def results(request):
    inp_value = request.GET.get('results', 'default value') 
    #Input term will be stored in the term file
    f=open("/home/ukancha/term", "w+")
    f.write(inp_value)
    f.close()
    os.system("$HADOOP_HOME/bin/hadoop dfs -rmr relevantDocOutput")
    os.system("$HADOOP_HOME/bin/hadoop dfs -rmr relevantDocScoreOutput")
    os.system("$HADOOP_HOME/bin/hadoop dfs -rmr topRankDocOutput")
    os.system("$HADOOP_HOME/bin/hadoop dfs -rmr contentOutput")
    if(os.path.isdir("/home/ukancha/NewOutput/topRankDocOutput")):
        os.system("rm -r /home/ukancha/NewOutput/topRankDocOutput")
    if(os.path.isdir("/home/ukancha/NewOutput/contentOutput")):
        os.system("rm -r /home/ukancha/NewOutput/contentOutput")
    os.system("hadoop jar /home/ukancha/PS6.jar")
    os.system("hadoop dfs -get topRankDocOutput /home/ukancha/NewOutput")
    os.system("hadoop dfs -get contentOutput /home/ukancha/NewOutput")
    ranks = open("/home/ukancha/NewOutput/topRankDocOutput/part-r-00000", "r").readlines()
    contents = open("/home/ukancha/NewOutput/contentOutput/part-r-00000", "r").readlines()
    
    xml = []
    for r in ranks:
        for c in contents:
            if(r.split('\t')[0] in c.split('\t',2)[0]):
                xml.append([r.split('\t')[0], r.split('\t')[1], c.split('\t', 2)[2]])
    context = {'inp_value': inp_value, 'xml' : xml}
    return render(request, 'results.html', context)

    
