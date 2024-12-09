import glob
import pdfplumber
import re
from collections import defaultdict
import json
from multiprocessing import Pool
import os

class PDFProcessor:
    def __init__(self, filepath):
        #以字典形式存储文本和表格
        self.filepath = filepath
        self.pdf = pdfplumber.open(filepath)
        
        self.all_text = defaultdict(dict)
        self.allrow = 0
        self.last_num = 0

    def check_lines(self, page, top, buttom):
        #这里的top和buttom分别指定上边界和下边界
        #这个extract方法用于提取页面中所有单词，其中包括[文本内容以及位置]
        lines = page.extract_words()[::]
        text = ''
        #上一行文本顶部
        last_top = 0
        #文本行位置差异
        last_check = 0
        #在指定页面区域中提取文本
        for l in range(len(lines)):
            each_line = lines[l]
            #用于匹配一行文本的结尾，包括句号分号，钱币单位，任意数字以及结尾标识符
            check_re = '(?:。|；|单位：元|单位：万元|币种：人民币|\d|报告(?:全文)?(?:（修订版）|（修订稿）|（更正后）)?)$'
            
            #如果没有指定上下边界，就处理整个界面
            if top == '' and buttom == '':
                #没有边界区域，如果当前行与上一行的顶部位置差异小于等于2，算作一个段落
                #在pdf里面，这里通过计算文本行之间的垂直间距(行)来判断是否为同一段落
                if abs(last_top - each_line['top']) <= 2:
                    #将当前行的文本内容追加到text里
                    text = text + each_line['text']
                #这里是检查lastcheck; 当前行的顶部位置低于页面高度的90%; 结尾不是既定的文本结尾模式
                elif last_check > 0 and (page.height * 0.9 - each_line['top']) > 0 and not re.search(check_re, text):
                    text = text + each_line['text']
                    
                else:
                    #此外，那就先加入换行符再追加文本
                    text = text + '\n' + each_line['text']
                    
            #只有下边界的时候进行处理
            elif top == '':
                #当前行在页面底部上方
                if each_line['top'] > buttom:
                    #同上，在一个段落中，追加txt
                    if abs(last_top - each_line['top']) <= 2:
                        text = text + each_line['text']
                    #
                    elif last_check > 0 and (page.height * 0.85 - each_line['top']) > 0 and not re.search(check_re, text):
                        text = text + each_line['text']
                        
                    else:
                        text = text + '\n' + each_line['text']
            
            #上下边界都进行了指定
            else:
                
                if each_line['top'] < top and each_line['top'] > buttom:
                    #否则
                    if abs(last_top - each_line['top']) <= 2:
                        text = text + each_line['text']
                    elif last_check > 0 and (page.height * 0.85 - each_line['top']) > 0 and not re.search(check_re, text):
                        text = text + each_line['text']
                    else:
                        text = text + '\n' + each_line['text']
                        
                        
            #当前行处理完，将lasttpo更新为当前行的顶部位置，并计算当前行的水平位置差异
            last_top = each_line['top']
            #更新last_check，这表示当前行的左侧位置与页面宽度的0.85倍的差值
            last_check = each_line['x1'] - page.width * 0.85

        return text


    # 删除所有列为空数据的列
    def drop_empty_cols(self, data):
        #将原始的行数据转置为列数据
        transposed_data = list(map(list, zip(*data)))
        #对弄出来的列，过滤掉空列
        filtered_data = [col for col in transposed_data if not all(cell == '' for cell in col)]
        #再次将列转换为行
        result = list(map(list, zip(*filtered_data)))
        return result


    def extract_text_and_tables(self, page):
        buttom = 0
        #这个用来获取所有表格
        tables = page.find_tables()
        if len(tables) >= 1:
            #有表格，就计数
            count = len(tables)
            for table in tables:
                
                #如果表格的下边界坐标[3]比现在的边界小(在这个上面)，就是已经处理过的了
                if table.bbox[3] < buttom:
                    #buttom是上一个表格的底部位置
                    pass
                
                #否则就是没处理的表格
                else:
                    count -= 1
                    top = table.bbox[1] #这个[1]就是上边界坐标
                    #获取txt
                    text = self.check_lines(page, top, buttom)
                    text_list = text.split('\n')
                    
                    #遍历每一行，以特定结构存储到字典里面
                    for _t in range(len(text_list)):
                        self.all_text[self.allrow] = {'page': page.page_number, #页码
                                                      'allrow': self.allrow, #行号
                                                      'type': 'text', #内容类型
                                                      'inside': text_list[_t]} #内容
                        self.allrow += 1

                    buttom = table.bbox[3]
                    #获取表格数据
                    new_table = table.extract()
                    r_count = 0
                    
                    for r in range(len(new_table)):
                        row = new_table[r]
                        #按行遍历，第一格空
                        if row[0] is None:
                            #记录连续空行数，用于合并
                            r_count += 1
                            
                            for c in range(len(row)):
                                #这里c是列号
                                #这个格不是None, ''或者' '空格
                                if row[c] is not None and row[c] not in ['', ' ']:
                                    
                                    
                                    if new_table[r - r_count][c] is None:
                                        #上一非空行的对应单元格为空，直接赋值
                                        new_table[r - r_count][c] = row[c]
                                        
                                    else:
                                        #不为空，将当前内容追加到上一行对应格里面
                                        new_table[r - r_count][c] += row[c]
                                        
                                    #当前格已处理完毕，标记这个格为空
                                    new_table[r][c] = None
                                    
                        #第一格非空，本行为有效行，重置空格计数器为0
                        else:
                            r_count = 0

                    end_table = []
                    #对刚才得到的表进行处理
                    for row in new_table:
                        #第一个格非空，进行处理
                        if row[0] != None:
                            cell_list = []
                            cell_check = False
                            for cell in row:
                                if cell != None:
                                    #去除有效格中的换行符
                                    cell = cell.replace('\n', '')
                                else:
                                    #是None的时候一律替换为''
                                    cell = ''
                                if cell != '':
                                    #行里有非空格，记录为有效行
                                    cell_check = True
                                    
                                #结果放入格列表
                                cell_list.append(cell)
                                
                            if cell_check == True:
                                #将格列表(处理后的有效行内容)放入[最终表]里
                                end_table.append(cell_list)
                                
                    #去除掉全部为空的列
                    end_table = self.drop_empty_cols(end_table)

                    for row in end_table:
                        #将提取的表内容整合进纯txt结果中
                        self.all_text[self.allrow] = {'page': page.page_number, 
                                                      'allrow': self.allrow,
                                                      'type': 'excel', 
                                                      'inside': str(row)}
                        
                        # self.all_text[self.allrow] = {'page': page.page_number, 'allrow': self.allrow, 'type': 'excel',
                        #                               'inside': ' '.join(row)}
                        self.allrow += 1

                    #处理完表格后，对剩余文本进行处理，跟上面差不多
                    if count == 0:
                        text = self.check_lines(page, '', buttom)
                        text_list = text.split('\n')
                        for _t in range(len(text_list)):
                            self.all_text[self.allrow] = {'page': page.page_number, 
                                                          'allrow': self.allrow,
                                                          'type': 'text', 
                                                          'inside': text_list[_t]}
                            self.allrow += 1
                            
        #没有表就只提取纯txt内容
        else:
            text = self.check_lines(page, '', '')
            text_list = text.split('\n')
            for _t in range(len(text_list)):
                self.all_text[self.allrow] = {'page': page.page_number, 
                                              'allrow': self.allrow, 
                                              'type': 'text', 
                                              'inside': text_list[_t]}
                self.allrow += 1

        #页眉页脚的正则表达
        #排除'计'字符；匹配页眉‘报告’，可选择地带有全文或几个括号内容；且需要'$'行尾结束
        first_re = '[^计](?:报告(?:全文)?(?:（修订版）|（修订稿）|（更正后）)?)$'
        #单独的'^'表示行的开始；匹配任意数字，斜杠与反斜杠，文字页数描述，横杠字符以及空格；需要前面这些东西至少出现一次
        end_re = '^(?:\d|\\|\/|第|共|页|-|_| ){1,}'
        
        if self.last_num == 0:
            #处理的第一页
            try:
                first_text = str(self.all_text[1]['inside'])
                end_text = str(self.all_text[len(self.all_text) - 1]['inside'])
                #符合页眉正则且页脚不包含'[',将这东西标记为‘页眉’
                #这里可能是因为页脚往往不包括需要[]的内容或者方括号在文档中有特殊意义
                if re.search(first_re, first_text) and not '[' in end_text:
                    self.all_text[1]['type'] = '页眉'
                    #符合页脚正则且页脚不包含'[',将这东西标记为'页脚'
                    if re.search(end_re, end_text) and not '[' in end_text:
                        self.all_text[len(self.all_text) - 1]['type'] = '页脚'
            # except:
            #     print(page.page_number)
            
            except Exception as e:
                print(f"Error on page {page.page_number}: {e}")
            
            
        else:
            try:
                #这里需要从当前页第二行文本进行提取
                first_text = str(self.all_text[self.last_num + 2]['inside'])
                end_text = str(self.all_text[len(self.all_text) - 1]['inside'])
                if re.search(first_re, first_text) and '[' not in end_text:
                    self.all_text[self.last_num + 2]['type'] = '页眉'
                if re.search(end_re, end_text) and '[' not in end_text:
                    self.all_text[len(self.all_text) - 1]['type'] = '页脚'
            
            #except:
                #print(page.page_number)
                
            except Exception as e:
                print(f"Error on page {page.page_number}: {e}")
                
                
        #获取最后一个alltext条目的索引
        self.last_num = len(self.all_text) - 1


    def process_pdf(self):
        #根据页提取txt和表格内容
        for i in range(len(self.pdf.pages)):
            self.extract_text_and_tables(self.pdf.pages[i])

    def save_all_text(self, path):
        for key in self.all_text.keys():
            with open(path, 'a+', encoding='utf-8') as file:
                file.write(json.dumps(self.all_text[key], ensure_ascii=False) + '\n')

def process_file(file_path):
    
    try:
        print('start ', file_path)
        processor = PDFProcessor(file_path)
        processor.process_pdf()
        #保存路径是alltxt2底下，很多个各自的路径
        #save_path = 'alltxt2/' + file_path.split('/')[-1].replace('.pdf', '.txt')
        save_path = os.path.join('alltxt', os.path.basename(file_path).replace('.pdf', '.txt'))
        processor.save_all_text(save_path)
        print('finish ', save_path)
        
    except:
        print('Error processing file:', file_path)
        print('check')


if '__main__' == __name__:
    folder_path = 'allpdf'
    #获取所有路径，生成文件路径列表，再进行逆序排列
    file_paths = glob.glob(f'{folder_path}/*')
    
    
    file_paths = sorted(file_paths, reverse=True)
    
    with Pool(processes=15) as pool:
        results = pool.map(process_file, file_paths)