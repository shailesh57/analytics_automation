from datetime import datetime
from math import ceil
@outputSchema("int")
def DateDiff(d1,d2,days=1.0):
	try:
		if str(d1)[0:10] == "11000101" or str(d2)[0:10] == "11000101" or d1 is None or d2 is None:
			return 0
		else:
			d = (datetime.strptime(d1[0:10],'%Y%m%d')-datetime.strptime(d2[0:10],'%Y%m%d')).days/days
			return int(ceil(d))
	except ValueError:
		return 0


@outputSchema("var1: chararray")
def add_period_proc(grp):
	if '.' in list(grp):
		return str(grp)
	elif len(grp) == 4:
		return (str(grp[0:2]+'.'+grp[2:4]))
	elif len(grp) == 3:
		return (str(grp[0:2]+'.'+grp[2:3]))
	else:
		return str(grp)

@outputSchema("var1: chararray")
def remove_first_undersore_exists(grp):
	if grp is None or grp is 'null':
		return 'no_data'
	l = len(grp)
	if grp[0] == '_':
		return grp[1:l]
	else:	
		return grp


@outputSchema("var1: chararray")
def remove_period_proc(g):
	if g is None or g is 'null':
		return 'no_data'
	proc = ''
	l = len(g)
	if g[0] == '_':
		grp=g[1:l]
	else:	
		grp=g
	for i in grp:
		if i != '.':
			proc=proc+i
	return proc


@outputSchema("t:tuple(diag1: chararray,diag2: chararray,diag3: chararray,diag4: chararray,diag5: chararray,diag6: chararray,diag7: chararray,diag8: chararray,diag9: chararray,diag10: chararray,diag11: chararray,diag12: chararray,diag13: chararray,diag14: chararray,diag15: chararray,diag16: chararray,diag17: chararray,diag18: chararray,diag19: chararray,diag20: chararray,diag21: chararray,diag22: chararray,diag23: chararray,diag24: chararray,diag25: chararray,ediag1: chararray,ediag2: chararray,ediag3: chararray,ediag4: chararray,ediag5: chararray,ediag6: chararray,ediag7: chararray,ediag8: chararray,ediag9: chararray,ediag10: chararray)")
def diag_parser(grp):
    l=[]
    for item in grp.split('|'):
        if '.' in item:
            prec=item.split('.')[0]
            if prec not in l:
                l.append(prec)
        else:
            if item not in l:
                l.append(item)
    dump=[]
    edump=[]
    for diag in l:
        if diag is None or diag is '' or diag is 'null':
            continue
        else:
            if diag[:1] not in ('E'):
                dump.append('_'+str(diag))
            else:
                edump.append('_'+str(diag))
    if len(dump)>25:
        c = dump[0:25]
    else:
        c = dump
    if len(edump)>10:
        e = edump[0:10]
    else:
        e = edump
    nonelist1 = ['_no_data']*(25-len(c))
    nonelist2 = ['_no_data']*(10-len(e))
    diagdump = c + nonelist1
    ediagdump = e + nonelist2
    return tuple(diagdump + ediagdump)



@outputSchema("t:tuple(var1: chararray,var2: chararray,var3: chararray,var4: chararray,var5: chararray,var6: chararray,var7: chararray,var8: chararray,var9: chararray,var10: chararray)")
def proc_parser(grp):
	cnt = 10
	l = []
	for item in grp.split('|'):
		if item not in l:
			l.append(item)
	if "null" in l:
		l.remove("null")
	if len(l)>cnt:
		return tuple(l[0:cnt])
	else:
		n = ['_no_data']*(cnt-len(l))
		o = l+n
		return tuple(o)


import re
@outputSchema("t:tuple(var1: chararray,var2: chararray)")
def drg_soi(drg):
	r_soi='0'
	r_drg='000'
	if drg is None:
		drg='00000'
	l = len(drg)
	if len(drg) is 6:
		if (not re.search('000[0-9][0-9][0-9]$',drg) is None):
			r_drg=drg[3:6]
		elif (not re.search('00[1-9][0-9][0-9][1-4]$',drg) is None):
			r_drg=drg[2:5]
			r_soi=drg[5]
	elif len(drg) is 5:
		if (not re.search('00[0-9][0-9][0-9]$',drg) is None):
			r_drg=drg[2:5]
		elif (not re.search('0[1-9][0-9][0-9][1-4]$',drg) is None):
			r_drg=drg[1:4]
			r_soi=drg[4]
	elif len(drg) is 4:
		if (not re.search('0[0-9][0-9][0-9]$',drg) is None):
			r_drg=drg[1:4]
		elif (not re.search('[1-9][0-9][0-9][1-4]$',drg) is None):
			r_drg=drg[0:3]
			r_soi=drg[3]
	elif len(drg) is 3:
		if (not re.search('[0-9][0-9][0-9]$',drg) is None):
			r_drg=drg
	elif len(drg) is 2:
		if (not re.search('[0-9][1-9]$',drg) is None):
			r_drg='0'+drg
	elif len(drg) is 1:
		if (not re.search('[1-9]$',drg) is None):
			r_drg='00'+drg
	else:
		r_drg='000'
	a = []
	a.append(r_drg)
	a.append(r_soi)
	return tuple(a)


@outputSchema("t:tuple(var1: chararray,var2: chararray,var3: chararray,var4: chararray,var5: chararray,var6: chararray,var7: chararray,var8: chararray,var9: chararray,var10: chararray,var11: chararray,var12: chararray,var13: chararray,var14: chararray,var15: chararray,var16: chararray,var17: chararray,var18: chararray,var19: chararray,var20: chararray,var21: chararray,var22: chararray,var23: chararray,var24: chararray,var25: chararray)")
def pic_25_cols(grp):
	cnt = 25
	l = []
	for item in grp.split('|'):
		if item not in l:
			if item is not None:
				l.append('_'+item)
	if "null" in l:
		l.remove("null")
	if '_' in l:
		l.remove('_')
	if len(l)>cnt:
		return tuple(l[0:cnt])
	else:
		n = ['_no_data']*(cnt-len(l))
		o = l+n
		return tuple(o)


@outputSchema("t:tuple(var1: chararray,var2: chararray,var3: chararray,var4: chararray,var5: chararray,var6: chararray,var7: chararray,var8: chararray,var9: chararray,var10: chararray,var11: chararray,var12: chararray)")
def pic_12_cols(grp):
	cnt = 12
	l=[]
	for item in grp.split('|'):
		if item not in l:
			if item is not None:
				l.append('_'+item)
	if "null" in l:
		l.remove("null")
	if '_' in l:
		l.remove('_')
	if len(l)>cnt:
		return tuple(l[0:cnt])
	else:
		n = ['_no_data']*(cnt-len(l))
		o = l+n
		return tuple(o)


@outputSchema("t:tuple(var1: chararray,var2: chararray,var3: chararray,var4: chararray,var5: chararray,var6: chararray,var7: chararray,var8: chararray,var9: chararray,var10: chararray)")
def pic_10_cols(grp):
	cnt = 10
	l=[]
	for item in grp.split('|'):
		if item not in l:
			if item is not None:
				l.append('_'+item)
	if "null" in l:
		l.remove("null")
	if '_' in l:
		l.remove('_')
	if len(l)>cnt:
		return tuple(l[0:cnt])
	else:
		n = ['_no_data']*(cnt-len(l))
		o = l+n
		return tuple(o)


@outputSchema("var1: chararray")
def pic_1_col(grp):
	l=[]
	if len(grp)>0:
		for item in grp.split('|'):
			if item is not None:
				for ch in item:		
					if '.' in item:
						prec=item.split('.')[0]
						if prec not in l:
							l.append('_'+prec)
					else:
						if item not in l:
							l.append('_'+item)
		if 'null' in l:
			l.remove('null')
		if len(l)>0:
			return l[0]
		else:
			return '_no_data'
	else:
		return '_no_data'