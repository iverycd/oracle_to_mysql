# -*- coding:utf-8 -*-
"""
  Author  : 'longguangbin'
  Contact : lgb453476610@163.com
  Date    : 2018/10/6
  Usage   : Format the sql string.
            Reference : https://github.com/sethelliott/sql_format
"""

import re


def conf_keywords():
    """Conf the wrap line key words. """
    wrap_keywords = ['left hash join', 'hash join', 'left join', 'inner join', 'outer join', 'right join', 'where',
                     'group by', 'order by', 'select', 'from', 'having', 'join', 'on', 'using', 'with']

    func_keywords = ['sum', 'min', 'max', 'abs', 'corr', 'count', 'exp', 'lag', 'lead', 'length', 'log',
                     'sqrt', 'stddev', 'stddev_pop', 'stddev_samp','then', 'end',
                     'case', 'cast', 'date_format', 'else', 'concat', 'concat_ws', 'split', 'date_add', 'date_sub',
                     'datediff', 'row_number', 'struct', 'substring', 'to_date', 'trim', 'trunc', 'when', 'window',
                     'year', 'month', 'minute', 'as', 'desc', 'asc', 'descending', 'ascending']
    return wrap_keywords, func_keywords


def clean_sql(sql):
    """Clean the sql, remove all \n to flat the sql. """
    flat_sql = sql.replace('\n', ' ').strip()
    return flat_sql


def find_keywords(sql, keywords):
    """Find all the keywords in `sql`.

    :param sql: The sql string being manipulated.
    :param keywords: A list of all the keywords being found in sql.
    :return: A dictionary with keys of sql keywords and values of two element lists with the starting and ending indexes of the keywords.
    """
    keyword_positions = []
    sql_len = len(sql)
    jude_list = [' ', '\n', '(', ')', '\t', ',']
    for n in range(sql_len):
        for kw in keywords:
            if sql[n:n + len(kw)].lower() == kw.lower():
                pre_single = False
                suf_single = False
                if (n == 0) or (sql[n - 1] in jude_list):
                    pre_single = True
                if (n == (sql_len - len(kw))) or (sql[n + len(kw)] in jude_list):
                    suf_single = True
                single = all([pre_single, suf_single])
                if single:
                    keyword_positions.append([sql[n:n + len(kw)], n, n + len(kw)])

    to_delete = []
    for kw1 in keyword_positions:
        for kw2 in keyword_positions:
            if (kw1[0].lower() in kw2[0].lower()) & (len(kw1[0]) < len(kw2[0])) & (kw1[1] >= kw2[1]) & (
                    kw2[2] <= kw2[2]):
                to_delete.append(kw1)

    for n in to_delete:
        if n in keyword_positions:
            keyword_positions.remove(n)

    keyword_positions = sorted(keyword_positions, key=lambda x: x[1])
    return keyword_positions


def line_keywords(sql, keyword_loc, wrap_keywords):
    """Add the \n to each key words.

    :param sql: The sql string being manipulated.
    :param keyword_loc: The location of key words.
    :param wrap_keywords: Wrap keywords.
    :return: String
    """
    offset = 0
    for i in keyword_loc:
        if i[0].lower() in wrap_keywords:
            sql = sql[:(i[1] + offset)] + '\n' + i[0] + '\n' + sql[(i[2] + offset):]
            offset += 2

    sql = sql.replace('\n\n', '\n')
    if sql[0] == '\n':
        sql = sql.replace(sql, sql[1:len(sql)])
    return sql


def line_wrap_add(sql, wrap_add):
    """Add the addition wrap line key, such as ',' . """
    for wrap in wrap_add:
        sql = sql.replace(wrap, wrap + '\n')
    return sql


def split_wrap(sql):
    """Split with \n, and strip the ' '. """
    sql_list = sql.split('\n')
    if sql_list[0] == '':
        del sql_list[0]
    if sql_list[-1] == '':
        del sql_list[-1]
    sql_list = list(map(lambda x: x.strip(), sql_list))
    return sql_list


def str_mode_change(sql_list, mode, wrap_keywords, func_keywords):
    """Change the key words. Upper, lower or change nothing. """
    if mode.lower() == 'none':
        return sql_list
    else:
        for i, frag in enumerate(sql_list):
            if frag.lower() in wrap_keywords:
                sql_list[i] = frag.lower() if mode.lower() == 'lower' else frag.upper()
                continue
            func_list = []
            for func_v in func_keywords:
                if func_v in frag.lower():
                    func_list.append(func_v)
            if len(func_list) > 0:
                mark_frag = frag.lower() if mode.lower() == 'lower' else frag.upper()
                func_loc = find_keywords(frag, func_list)
                for loc in func_loc:
                    frag = frag[:loc[1]] + mark_frag[loc[1]:loc[2]] + frag[loc[2]:]
                sql_list[i] = frag
        return sql_list


def add_indent(sql_list, wrap_keywords):
    """Add the indent. """
    indent_space = '    '
    default_num = 1
    inner_num = 0
    count_left = 0
    count_right = 0
    for i, frag in enumerate(sql_list):
        if frag.lower() not in wrap_keywords:
            sql_list[i] = (default_num + inner_num * 2) * indent_space + frag
        if (frag.lower() in wrap_keywords) and (inner_num > 0):
            sql_list[i] = inner_num * 2 * indent_space + frag
        count_left += len(re.findall('\(', frag))
        count_right += len(re.findall('\)', frag))
        inner_num = count_left - count_right
    return sql_list


def sql_format(sql, wrap_add=None, mode='none'):
    """Format the sql string.

    :param sql: The input sql
    :param wrap_add: Add some string to wrap a line, such as ',', 'and'
    :param mode: 'none', 'upper', 'lower'. key words lower(upper), no change.
    :return: Formatted sql string.
    """
    if not wrap_add:
        wrap_add = [',']
        # wrap_add = []

    wrap_keywords, func_keywords = conf_keywords()

    flat_sql = clean_sql(sql)
    wrap_keywords_loc = find_keywords(flat_sql, wrap_keywords)

    sql = line_keywords(flat_sql, wrap_keywords_loc, wrap_keywords)
    sql = line_wrap_add(sql, wrap_add)
    sql_list = split_wrap(sql)
    sql_list = str_mode_change(sql_list, mode, wrap_keywords, func_keywords)
    sql_list = add_indent(sql_list, wrap_keywords)

    format_sql = '\n'.join(sql_list)

    return format_sql


def read_sample_sql():
    with open('test_sql.sql') as f:
        raw_sql = f.read()
    return raw_sql


def write_sample_sql(sql):
    with open('test_format_sql.sql', 'w') as f:
        f.write(sql)


def sample_test():
    raw_sql = read_sample_sql()
    print('\n\n(1) [ raw_sql ]: \n')
    print(raw_sql)
    print('\n\n(2) [ format_sql ]: \n')
    format_sql = sql_format(raw_sql, wrap_add=[',', 'and'])
    print(format_sql)
    print('\n\n(3) [ lower key words format_sql ]: \n')
    format_sql = sql_format(raw_sql, wrap_add=[',', 'and'], mode='lower')
    print(format_sql)
    print('\n\n(4) [ upper key words format_sql ]: \n')
    format_sql = sql_format(raw_sql, wrap_add=[',', 'and'], mode='upper')
    print(format_sql)
    write_sample_sql(format_sql)


if __name__ == '__main__':
    sample_test()

