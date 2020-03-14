import pandas as pd
import math
from collections import defaultdict
import json
import pymysql


def nested_defaultdict():
    return defaultdict(nested_defaultdict)


program_outcomes, course_outcomes, student_list, exams = dict(), dict(), dict(), defaultdict(nested_defaultdict)
course_code, course_name, course_credit = '', '', ''
'''
exams:(dict)
    exam name:(key) str
    exam percentage: int
    questions:(dict)      
                    question name:(key) int
                    question percentage: int
                    related outcomes: str -> list
                    count question?: str

'''


def analyze_spa(file_path):
    program_sheet(file_path)
    course_sheet(file_path)
    grades_sheet(file_path)
    print(course_code, course_name, course_credit)
    a = json.dumps(exams)
    b = json.loads(a)
    print(b)


def program_sheet(file_path):
    global program_outcomes
    data = pd.read_excel(file_path, sheet_name=0, skiprows=1)
    df = pd.DataFrame(data, columns=['Program Outcomes', 'Program Outcome Explanation'])
    program_outcomes = df.set_index('Program Outcomes')['Program Outcome Explanation'].to_dict()


def course_sheet(file_path):
    global course_outcomes, course_code, course_name, course_credit
    data = pd.read_excel(file_path, sheet_name=1, skiprows=5, index_col='Course Outcomes')
    df = pd.DataFrame(data, columns=['Course Outcome Explanation', 'Program Outcomes'])
    course_outcomes = df.to_dict()
    data = pd.read_excel(file_path, sheet_name=1, header=None)
    df = pd.DataFrame(data)
    course_code, course_name, course_credit = df.iat[0, 1], df.iat[1, 1], df.iat[2, 1]


def grades_sheet(file_path):
    global course_outcomes, course_code, course_name, course_credit, exams
    data = pd.read_excel(file_path, sheet_name=2, skiprows=9, header=None)
    df = pd.DataFrame(data)
    print(df.shape)
    for i in range(5, df.shape[1]):
        col = df[i][:].tolist()
        print(col)
        #col = each students grade on i'th question/column

    '''
    Following part of code creates a dictionary as the template below.
    {
       "[Exam Name]":{
          "Exam Percentage:":int,
          "Questions":{
             "[Question Name]":{
                "Question Percentage:":int,
                "Related Outcomes:":str -> list,
                "Count Question?:":int
             }, ...
          }
        }, ...
    }
    '''
    data = pd.read_excel(file_path, sheet_name=2, skiprows=1, header=None, nrows=6)
    df = pd.DataFrame(data).set_index(3)
    current_exam = ''
    for i in range(5, df.shape[1] + 1):
        col = df[i][:].tolist()
        if not isinstance(col[0], float):
            current_exam = col[0]
            exams[current_exam]['Exam Percentage:'] = col[1]
        exams[current_exam]['Questions'][col[2]] = {'Question Percentage:':col[3], 'Related Outcomes:': col[4], 'Count Question?:': col[5]}


analyze_spa('/Users/alpgokcek/PycharmProjects/rest-test/input.xlsx')
#print(course_code, course_name, course_credit)
a = json.dumps(exams)
b = json.loads(a)
#print(b)
'''
conn = pymysql.connect(host='eu-cdbr-west-02.cleardb.net', user='baa32c7d78f3b0', passwd='225864ad', db='heroku_40d639016a5bdb3')
cur = conn.cursor()
cur.execute("SELECT * FROM user")
for r in cur:
    print(r)
cur.close()
conn.close()
'''