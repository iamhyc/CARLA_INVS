#!/usr/bin/env python3
## amend relative import
import sys
from pathlib import Path
sys.path.append( Path(__file__).resolve().parent.as_posix() ) #file path
## amend SUMO path
import os, sys
if 'SUMO_HOME' in os.environ:
    SUMO_LIB = Path(os.environ['SUMO_HOME'], 'tools')
    sys.path.append( SUMO_LIB.as_posix() )
else:
    sys.exit("please declare environment variable 'SUMO_HOME'")
## normal import
import math
import shutil
import subprocess as sp
from halo import Halo
import lxml.etree as ET
from shapely.geometry import Polygon
from collections import Counter
import create_sumo_vtypes as vtypes
import netconvert_carla as net
from utils import *

ROOT_FOLDER = Path('../my_data')
GLOBAL_FOLDER = ROOT_FOLDER / 'global'; GLOBAL_FOLDER.mkdir(exist_ok=True, parents=True)
NET_FOLDER    = ROOT_FOLDER / 'net';    NET_FOLDER.mkdir(exist_ok=True, parents=True)
STAT_FOLDER   = ROOT_FOLDER / 'stat';   STAT_FOLDER.mkdir(exist_ok=True, parents=True)
ROU_FOLDER    = ROOT_FOLDER / 'rou';    ROU_FOLDER.mkdir(exist_ok=True, parents=True)
TRACES_FOLDER = ( ROOT_FOLDER / '..' / 'my_traces' ).resolve()
TRACES_FOLDER.mkdir(exist_ok=True, parents=True)

print_ = lambda x:print('[convert_main] {}'.format(x))
OPTIONS = [ 'GEN_VIEW', 'GEN_VTYPE', 'GEN_NET', 'GEN_STAT', 'GEN_ROU', 'RUN_TRACES' ]
O_FLAGS = [  False,      False,       False,     True,       True,      True        ]
STATISTICS = {
    # Total numberr of inhabitants
    "inhabitants": 1000,
    # Estimation of the time to drive 1km from bird eye's view (unit: second)
    "meanTimePerKmInCity": 360,
    # variance of the normal distribution for departure time (unit: second)
    "departureVariation": 300,
    # proportion of random traffic in the whole traffic demand
    "uniformRandomTraffic": 0.20,
}

#=====================================================#
if True: #for code folding
    from picotui.screen import Screen
    from picotui.context import Context
    from picotui.widgets import (Dialog, WCheckbox, WButton, ACTION_OK)

    _choices = ['%d. %s'%(i+1,x) for (i,x) in enumerate(OPTIONS)]
    _status  = [WCheckbox(x, choice=f) for x,f in zip(_choices,O_FLAGS)] 
    with Context(cls=True):
        _width  = 4 + max( [len(x) for x in _choices] )
        _height = 1 + len(_choices)
        d = Dialog( 0, 0, _width, _height )
        # add multiple checkbox
        for i,x in enumerate(_status):
            d.add(1, i+1, x)
        # add OK butthon
        b = WButton(_width-4, "OK")
        d.add(1+int((_width-b.w)/2), _height, b)
        b.finish_dialog = ACTION_OK
        # rendering
        res = d.loop()
        pass
    Screen.cls() #cleanup the screen
    Screen.goto(0, 0) #cleanup the screen

    CHOICES = dict()
    if res==ACTION_OK:
        for x in _status:
            _key = x.t.split()[1]
            CHOICES[_key] = x.choice
        # print(res, [x.choice for x in _status])
    else:
        exit()
    pass

#=====================================================#
with Halo(text='Generate viewsettings.xml file.') as sh:
    if CHOICES['GEN_VIEW']:
        view_file = Path('./data/viewsettings.xml')
        if view_file.exists():
            shutil.copy(view_file, GLOBAL_FOLDER)
            sh.succeed()
            # print_('viewsettings.xml file generated.')
        else:
            sh.fail()
            # print_('viewsettings.xml file missing.')
    else:
        sh.info('GEN_VIEW skipped.')
    pass    

#=====================================================#
with Halo(text='Generate vtypes.rou.xml file.') as sh:
    if CHOICES['GEN_VTYPE']:
        _obj = sp.run(['bash', '-c', 'ps -a | grep CarlaUE4'], capture_output=True)
        if b'CarlaUE4' not in _obj.stdout:
            sh.fail()
            print('You must have one running CarlaUE4 instance first!')
        else:
            vtype_args = vtypes.getArgParser().parse_args([
                            '--carla-map', 'Town03',
                            '--output-file', '%s'%(GLOBAL_FOLDER / 'carlavtypes.rou.xml')
                        ])
            try:
                vtypes.main(vtype_args)
            except Exception as e:
                sh.fail()
                raise e
            else:
                sh.succeed()
                # print_('vtypes.rou.xml file generated.')
                pass
    else:
        sh.info('GEN_VTYPE skipped.')
    pass

#=====================================================#
with Halo(text='Generate *.net.xml file.') as sh:
    if CHOICES['GEN_NET']:
        xodr_glob = Path('./data').glob('*.xodr')
        for xodr_file in xodr_glob:
            _file = '%s.net.xml'%xodr_file.stem
            sh.start(text='Generate %s file.'%_file)
            try:
                net.netconvert_carla(
                    xodr_file = (xodr_file).as_posix(),
                    output = str(NET_FOLDER / _file),
                    guess_tls = False
                )
            except Exception as e:
                sh.fail()
                # print_('%s.net.xml file failed.'%xodr_file.stem)
            else:
                sh.succeed()
                # print_('%s.net.xml file generated.'%xodr_file.stem)
            pass
    else:
        sh.info('GEN_NET skipped.')
    pass

#=====================================================#
def get_net_statistics(net_file):
    result = dict()
    net_tree = ET.parse(net_file)
    result['edges'] = dict()
    result['junctions'] = dict()
    ## get network size
    _size = [float(x) for x in net_tree.find('location').get('convBoundary').split(',')]
    result['size'] = _size
    ## get normal edges stat (id, from, to)
    for e in net_tree.iter('edge'):
        if e.get('function') != 'internal':
            _id, _from, _to = e.get('id'), e.get('from'), e.get('to')
            _length = float( e.find('lane').get('length') ) #length specified by first lane
            assert(_from is not None); assert(_to is not None)
            result['edges'].update({ _id:{'from':_from, 'to':_to, 'length':_length} })
            #
            if _from in result['junctions']:
                result['junctions'][_from]['out'].append(_id)
            else:
                result['junctions'][_from] = {'centroid':None, 'shape':None, 'in':[], 'out':[_id]}
            #
            if _to in result['junctions']:
                result['junctions'][_to]['in'].append(_id)
            else:
                result['junctions'][_to]   = {'centroid':None, 'shape':None, 'in':[_id], 'out':[]}
            pass
        pass
    ## get junction stat (id, centroid, shape)
    for jc in net_tree.iter('junction'):
        _id, _shape, _type = jc.get('id'), jc.get('shape'), jc.get('type')
        if _id not in result['junctions']:
            if _type!='internal':print('junction %s not connected.'%_id)
        else:
            if '.' in _id: #NOTE: ignore inter-connect junctions
                result['junctions'].pop(_id)
                continue
            _shape = [x.split(',') for x in _shape.split()]
            _shape = [ [float(x[0]), float(x[1])] for x in _shape]
            _c = Polygon([*_shape, _shape[0]]).centroid
            _centroid = (_c.x, _c.y)
            result['junctions'][_id]['centroid'] = _centroid
            result['junctions'][_id]['shape'] = _shape
            pass
        pass
    ##
    # import json
    # print(json.dumps(result, indent=4))
    return result

def generate_stat_xml(net_file):
    import numpy as np
    from sklearn.cluster import KMeans
    ## get net_file statistics
    _stat = get_net_statistics(net_file)
    root = ET.Element('city')
    ## expand <general> element
    _attribs = {
        "inhabitants"       : str( STATISTICS["inhabitants"] ),
        "households"        : str(int( 0.55*STATISTICS["inhabitants"] )),
        "childrenAgeLimit"  : "18", # (not used) for school-household traffic
        "retirementAgeLimit": "66",
        "carRate"           : "0.58",
        "unemploymentRate"  : "0.05", 
        "footDistanceLimit" : "250", # (not used)
        "incomingTraffic"   : "200", # number of traffic in to city
        "outgoingTraffic"   : "50",  # number of traffic out of city
        "laborDemand"       : "1.05"
    }
    _general = ET.SubElement(root, 'general', **_attribs)

    ## expand <parameters> element
    _attribs = {
        "carPreference"         : "1.00", # (no other transportation)
        "meanTimePerKmInCity"   : str( STATISTICS["meanTimePerKmInCity"] ),
        "freeTimeActivityRate"  : "0.15", # probability for one household to have a free-time activity
        "uniformRandomTraffic"  : str( STATISTICS["uniformRandomTraffic"] ),
        "departureVariation"    : str( STATISTICS["departureVariation"] )
    }
    _parameters = ET.SubElement(root, 'parameters', **_attribs)

    ## expand <population> element
    _population = ET.SubElement(root, 'population')
    ET.SubElement(_population, 'bracket', beginAge="0",  endAge="30", peopleNbr="30") # 30% in age [0,30)
    ET.SubElement(_population, 'bracket', beginAge="30", endAge="60", peopleNbr="50") # 50% in age [30, 60)
    ET.SubElement(_population, 'bracket', beginAge="60", endAge="90", peopleNbr="20") # 20% in age [60, 90)

    ## expand <workHours> element; FIXME: change to random
    _workHours = ET.SubElement(root, 'workHours')
    ET.SubElement(_workHours, 'opening', hour='30600', proportion="0.30") #30% starts working at 0830;
    ET.SubElement(_workHours, 'opening', hour='32400', proportion="0.70") #70% starts working at 0900.
    ET.SubElement(_workHours, 'closing', hour='43200', proportion="0.20") #20% stops working at 1200;
    ET.SubElement(_workHours, 'closing', hour='63000', proportion="0.20") #20% stops working at 1730;
    ET.SubElement(_workHours, 'closing', hour='64800', proportion="0.60") #60% stops working at 1800.

    ## prepare statistics
    _size = _stat['size']
    _ord = ( (_size[2]-_size[0])/2, (_size[3]-_size[1])/2 )
    dist = lambda x: math.sqrt(sum([ pow(p[0]-p[1], 2) for p in zip(x, _ord) ]))
    _choices = [ (key, len(val['in']), dist(val['centroid'])) for key,val in _stat['junctions'].items() ]
    _choices.sort(key=lambda x:x[2]) #sort by distance to city center

    ## expand <streets> element
    _streets = ET.SubElement(root, 'streets')
    #
    # allocate edges for work position
    _choices_dist = np.array( list(zip(*_choices))[2] ).reshape(-1,1)
    _choices_cluster = KMeans(2).fit(_choices_dist).labels_
    _tmp_idx = np.argwhere( _choices_cluster == _choices_cluster[-1] )[0,0]
    _work_junctions = list(zip(*_choices[:_tmp_idx]))[0] #only use 'id'
    _work_edges = list()
    for j in _work_junctions:
        _work_edges.extend( _stat['junctions'][j]['in'] )
        # _work_edges.extend([ e.lstrip('-') for e in _stat['junctions'][j]['in'] ])
    _work_edges = Counter( _work_edges )
    for edge,val in _work_edges.items():
        ET.SubElement(_streets, 'street', edge=edge, population=str(val), workPosition=str(val*10))
    #
    # allocate edges for households
    _choices_cluster = KMeans(3).fit(_choices_dist).labels_
    _tmp_idx = np.argwhere( _choices_cluster == _choices_cluster[-1] )[0,0]
    _house_junctions = list(zip(*_choices[_tmp_idx:]))[0] #only use 'id
    _house_edges = list()
    for j in _house_junctions:
        _house_edges.extend( _stat['junctions'][j]['out'] )
        # _house_edges.extend([ e.lstrip('-') for e in _stat['junctions'][j]['out'] ])
    _house_edges = Counter( _house_edges )
    for edge,val in _house_edges.items():
        ET.SubElement(_streets, 'street', edge=edge, population=str(val*10), workPosition=str(val))

    ## expand <cityGates> element
    _cityGates = ET.SubElement(root, 'cityGates')
    for edge,_ in _house_edges.most_common()[::-1]:
        _edge = '-'+edge if ('-' not in edge) else edge.lstrip('-')
        if _edge in _stat['edges']:
            e_len = str( _stat['edges'][edge]['length']/2 )
            _e_len = str( _stat['edges'][_edge]['length']/2 )
            ET.SubElement(_cityGates, 'entrance', edge=edge, incoming="0.5", outgoing="0.5", pos=e_len)
            ET.SubElement(_cityGates, 'entrance', edge=_edge, incoming="0.5", outgoing="0.5", pos=_e_len)
            break
        pass

    ## return
    # print(_choices)
    # print( ET.tostring(root, pretty_print=True).decode('utf-8') )
    return root

# generate_stat_xml('../my_data/net/Town02.net.xml') #for test purpose
with Halo(text='Generate *.stat.xml file.') as sh:
    if CHOICES['GEN_STAT']:
        net_file_glob = NET_FOLDER.glob('*.net.xml')
        for net_file in net_file_glob:
            _name = net_file.name.split('.net.xml')[0]
            stat_xml = generate_stat_xml( net_file.as_posix() )
            stat_xml_tree = ET.ElementTree(stat_xml)
            stat_xml_tree.write( '%s.stat.xml'%(STAT_FOLDER/_name), pretty_print=True )
            pass
        sh.succeed()
        pass
    else:
        sh.info('GEN_STAT skipped.')
    pass

#=====================================================#
with Halo(text='Generate *.rou.xml file.') as sh:
    if CHOICES['GEN_ROU']:
        stat_file_glob = STAT_FOLDER.glob('*.stat.xml')
        for stat_file in stat_file_glob:
            _name = stat_file.name.split('.stat.xml')[0]
            net_file = NET_FOLDER / ('%s.net.xml'%_name)
            _prefix = ROU_FOLDER / _name
            #
            _obj = sp.run(['activitygen',
                    '--net-file', str(net_file),
                    '--stat-file', str(stat_file),
                    '--output-file', '%s.trips.rou.xml'%_prefix,
                    '--random']
            , capture_output=True)
            #
            _obj = sp.run(['duarouter',
                    '--net-file', str(net_file),
                    '--route-files', '%s.trips.rou.xml'%_prefix,
                    '--output-file', '%s.rou.xml'%_prefix,
                    '--ignore-errors']
            , capture_output=True)
            pass
        sh.succeed()
    else:
        sh.info('GEN_ROU skipped.')
    pass

#=====================================================#
with Halo(text='Generate *.sumocfg file.') as sh:
    rou_file_glob = ROU_FOLDER.glob('*.rou.xml')
    rou_file_glob = filter(lambda x:'.trips' not in str(x), rou_file_glob)
    for rou_file in rou_file_glob:
        name = rou_file.name.split('.rou.xml')[0]
        NS = 'http://www.w3.org/2001/XMLSchema-instance'
        _attr = '{%s}noNamespaceSchemaLocation'%(NS)
        _xsd = 'http://sumo.dlr.de/xsd/sumoConfiguration.xsd'
        root = ET.Element('configuration', attrib={_attr: _xsd})
        #
        _input = ET.SubElement(root, 'input')
        ET.SubElement(_input, 'net-file', value='net/%s.net.xml'%name)
        ET.SubElement(_input, 'route-files', value='global/carlavtypes.rou.xml,rou/%s.rou.xml'%name)
        #
        _gui_only = ET.SubElement(root, 'gui_only')
        ET.SubElement(_gui_only, 'gui-settings-file', value='global/viewsettings.xml')
        #
        root_tree = ET.ElementTree(root)
        root_tree.write( '%s.sumocfg'%(ROOT_FOLDER/name), pretty_print=True, xml_declaration=True, encoding='UTF-8' )
        pass
    sh.succeed()
    pass

#=====================================================#
with Halo(text='Run and Generate traces file.') as sh:
    if CHOICES['RUN_TRACES']:
        cfg_file_glob = ROOT_FOLDER.glob('*.sumocfg')
        for cfg_file in cfg_file_glob:
            _name = cfg_file.name.split('.sumocfg')[0]
            fcd_file = '%s_fcd.xml'%(TRACES_FOLDER/_name)
            #
            sh.start(text='[%s] Run trace simulation.'%_name)
            _obj = sp.run(['sumo',
                '-c', cfg_file,
                '--fcd-output', fcd_file
            ], capture_output=True)
            #
            sh.text = '[%s] Collect trace files.'%_name
            with WorkSpace(SUMO_LIB) as ws:
                _obj = sp.run(['./traceExporter.py',
                    '--fcd-input', fcd_file,
                    '--omnet-output', '%s_omnet.xml'%(TRACES_FOLDER/_name)
                    # '--ipg-output', '%s_ipg.txt'%(TRACES_FOLDER/_name)
                ], capture_output=True)
            #
            sh.succeed()
            pass
    else:
        sh.info('RUN_TRACES skipped.')
    pass
