#!/usr/bin/env python3
import os
import sys
import time
import glob
# amend relative import
from pathlib import Path
sys.path.append( Path(__file__).resolve().parent.parent.as_posix() ) #repo path
sys.path.append( Path(__file__).resolve().parent.as_posix() ) #file path
from params import *

try:
    _egg_file = sorted(Path(CARLA_PATH, 'PythonAPI/carla/dist').expanduser().glob('carla-*%d.*-%s.egg'%(
        sys.version_info.major,
        'win-amd64' if os.name == 'nt' else 'linux-x86_64'
    )))[0].as_posix()
    sys.path.append(_egg_file)
except IndexError:
    print('CARLA Egg File Not Found.')
    exit()
#====================================================================#
import carla
import random
import weakref
import logging
import argparse
# import mayavi.mlab

import open3d as o3d
import matplotlib.pyplot as plt
import numpy as np
from threading import Thread
from carla import ColorConverter as cc
import matplotlib.pyplot as plt     
from mpl_toolkits.mplot3d import Axes3D
#====================================================================#
SpawnActor = carla.command.SpawnActor
SetAutopilot = carla.command.SetAutopilot
FutureActor = carla.command.FutureActor
ApplyVehicleControl = carla.command.ApplyVehicleControl
Attachment = carla.AttachmentType
#====================================================================#
try:
    sys.path.append(Path(CARLA_PATH, 'PythonAPI/carla').expanduser().as_posix() )
    sys.path.append(Path(CARLA_PATH, 'PythonAPI/examples').expanduser() )
except IndexError:
    pass

from agents.navigation.behavior_agent import BehaviorAgent  # pylint: disable=import-error
from agents.navigation.roaming_agent import RoamingAgent  # pylint: disable=import-error
from agents.navigation.basic_agent import BasicAgent  # pylint: disable=import-error

from utils.get2Dlabel import ClientSideBoundingBoxes
#====================================================================#

DBG=True

class Configuration:
    def __init__(self, prefix):
        import yaml
        _config = yaml.load( open('config.yaml','r') )

        self._prefix = prefix
        self._config = dict()
        for k,v in _config.items():
            if k.startswith(prefix):
                _key = k.lstrip(prefix+'_').capitalize()
                self._config.update( {_key:v} )
        pass

    def __getattribute__(self, name: str):
        try:
            return super().__getattribute__(name)
        except:
            try:
                _key, _id = name.split('_')
                _key = _key.capitalize()
                return self._config[_key][_id]
            except:
                raise Exception('No item "%s" found for %s_%s'%(_id, self._prefix, _key))
                # return None
        pass

class CAVcollect_Thread(Thread):
    def __init__(self, parent_id, sensor_attribute, sensor_transform, config, raw_data_path):
        Thread.__init__(self)
        self.sensor = None
        self._camera_transforms = sensor_transform#(sensor_transform, Attachment.Rigid)
        self.config = config
        self.raw_data_path = raw_data_path
        self.recording = False
        gamma_correction = 2.2
        Attachment = carla.AttachmentType
        #
        self.client = carla.Client(config.client_host, config.client_port)
        world = self.client.get_world()
        self._parent = world.get_actor(parent_id)
        #
        bp_library = world.get_blueprint_library()
        bp = bp_library.find(sensor_attribute[0])
        if sensor_attribute[0].startswith('sensor.camera'):
            bp.set_attribute('image_size_x', str(config.calibration_image_width))
            bp.set_attribute('image_size_y', str(config.calibration_image_height))
            if bp.has_attribute('gamma'):
                bp.set_attribute('gamma', str(gamma_correction))
            for attr_name, attr_value in sensor_attribute[3].items():
                bp.set_attribute(attr_name, attr_value)
        elif sensor_attribute[0].startswith('sensor.lidar'):
            bp.set_attribute('range', '100')
            bp.set_attribute('channels','64')
            bp.set_attribute('points_per_second','2240000')
            bp.set_attribute('rotation_frequency','20')
            bp.set_attribute('sensor_tick', str(0.05))
            bp.set_attribute('dropoff_general_rate', '0.0')
            bp.set_attribute('dropoff_intensity_limit', '1.0')
            bp.set_attribute('dropoff_zero_intensity', '0.0')
            # bp.set_attribute('noise_stddev', '0.0')
        sensor_attribute.append(bp)
        self.sensor_attribute = sensor_attribute
        pass
    
    def run(self):
        def _parse_image(weak_self, image, filename):
            self = weak_self()
            if image.frame % self.config.client_sample_frequency != 0:
                return
            if self.sensor.type_id.startswith('sensor.camera'):
                image.convert(self.sensor_attribute[1])
                image.save_to_disk(filename+'/%010d' % image.frame)
            else:
                image.save_to_disk(filename+'/%010d' % image.frame)
            pass
        #
        self.sensor = self._parent.get_world().spawn_actor(
            self.sensor_attribute[-1],
            self._camera_transforms[0],
            attach_to=self._parent)
            # attachment_type=self._c#amera_transforms[1])
        filename = Path(self.raw_data_path,
                        '%s_%d'%(self._parent.type_id, self._parent.id),
                        '%s_%d'%(self.sensor.type_id, self.sensor.id)
                    ).as_posix()
        #
        weak_self = weakref.ref(self)
        self.sensor.listen(lambda image: _parse_image(weak_self, image, filename))
        # self.sensor.stop()
        pass
    
    def get_sensor_id(self):
        self.join()
        return self.sensor.id
    
    pass

class SpawnWorker:
    def __init__(self, parent, time_step:float, sync:bool):
        self.parent = parent
        self.time_step = time_step
        self.sync = sync
        pass

    def start_look(self):
        _parent = self.parent
        self.synchronous_master = False
        if self.sync:
            settings = _parent.world.get_settings()
            if not settings.synchronous_mode:
                self.synchronous_master = True
                _parent.traffic_manager.set_synchronous_mode(True)
                settings.synchronous_mode = True
                settings.fixed_delta_seconds = self.time_step
                _parent.apply_settings(settings)
            else:
                self.synchronous_master = False
                print('synchronous_master is False.')
        #
        tmp_x, tmp_y = [], []
        for tmp_transform in _parent.map.initial_spawn_points:
            tmp_location = tmp_transform.location
            tmp_x.append( ((tmp_location.x-100)*-1)+100 )
            tmp_y.append( tmp_location.y )
        Map.plot_points(tmp_x,tmp_y)

    def stop_look(self):
        _parent = self.parent
        if self.sync:
            settings = _parent.world.get_settings()
            settings.synchronous_mode = False
            settings.fixed_delta_seconds = None
            _parent.world.apply_settings(settings)
        _parent.world = _parent.client.reload_world()
        _parent.map.initial_spectator(_parent.config.map_spectator_point)
    pass

class RecordWorker:
    def __init__(self, parent, filename, hd:list, av:list):
        self.parent = parent
        self.filename = filename
        self.hd_id = hd
        self.av_id = av
        pass

    def shuffle_spawn_points(self, spawn_points, start=False):
        # example:
        # av_id = [4,5,27,20,97,22,14,77,47]
        # hd_id = [19,21,29,31,44,48,87,96] + [i for i in range(50,70)]
        # random.shuffle(spawn_points)
        if self.parent.map.pretrain_model:
            cav = [spawn_points[i] for i in self.av_id]
            hd = [spawn_points[i] for i in self.hd_id]
            if len(cav) == 0 and len(hd) == 0:
                return spawn_points[:60],spawn_points[-20:]
            else:
                return hd, cav
        pass

    def spawn_actorlist(self, actor_type, agent_blueprint=None, spawn_points=None, parent_agent=None):
        _parent = self.parent
        bacth_spawn = []
        id_list = []
        if actor_type == 'vehicle':
            if not random.choice(agent_blueprint).id.startswith('vehicle.tesla'):
                # HD_agents
                # print(len(spawn_points))
                for n, transform in enumerate(spawn_points):
                        blueprint = random.choice(agent_blueprint)
                        while 'tesla' in blueprint.id or 'crossbike' in blueprint.id or 'low_rider' in blueprint.id:
                            blueprint = random.choice(agent_blueprint)
                        bacth_spawn.append(SpawnActor(blueprint, transform).then(SetAutopilot(FutureActor, True)))
                for response in _parent.client.apply_batch_sync(bacth_spawn, False):
                    if response.error:
                        print(response.error)
                        logging.error(response.error)
                    else:
                        tmp_vehicle = _parent.world.get_actor(response.actor_id)
                        if tmp_vehicle.bounding_box.extent.y < 0.6:
                            print(tmp_vehicle.bounding_box.extent.y)
                            tmp_vehicle.destroy()
                        # print(tmp_vehicle.bounding_box.extent.y)
                        if int(tmp_vehicle.attributes['number_of_wheels']) == 2:
                            tmp_vehicle.destroy()
                        else:
                            id_list.append(response.actor_id)
            elif random.choice(agent_blueprint).id.startswith('vehicle.tesla'):
                # CAV_agents
                for n, transform in enumerate(spawn_points):
                        blueprint = random.choice(agent_blueprint)
                        bacth_spawn.append(SpawnActor(blueprint, transform).then(SetAutopilot(FutureActor, True)))
                for response in _parent.client.apply_batch_sync(bacth_spawn, True):
                    if response.error:
                        logging.error(response.error)
                    else:
                        id_list.append(response.actor_id)
                        vehicle = _parent.client.get_world().get_actor(response.actor_id)
                        tmp_sensor_id_list = self.spawn_actorlist(
                            'sensor',
                            _parent.sensor_attribute,
                            _parent.sensor_transform,
                            response.actor_id
                        )
                        _parent.sensor_relation[str(response.actor_id)] = tmp_sensor_id_list
                        random.shuffle(_parent.map.destination)
                        tmp_agent = Vehicle_Agent(vehicle)
                        tmp_agent.set_destination(tmp_agent.vehicle.get_location(), _parent.map.destination[0].location, clean=True)
                        self.agent_list.append(tmp_agent)
        elif actor_type == 'sensor':
            # sensor agents
            for index in range(len(_parent.sensor_attribute)):
                sensor_attribute = _parent.sensor_attribute[index]
                transform = _parent.sensor_transform[index]
                tmp_sensor = CAVcollect_Thread(parent_agent, sensor_attribute, transform, _parent.config, _parent.raw_data_path)
                tmp_sensor.start()
                self.sensor_thread.append(tmp_sensor)
                id_list.append( tmp_sensor.get_sensor_id() )
        return id_list

    def start_record(self):
        _parent = self.parent
        self.synchronous_master = False
        self.dynamic_weather = False
        #
        if _parent.dynamic_weather:
            from dynamic_weather import Weather
            w = _parent.world.get_weather()
            w.precipitation = 80
            weather = Weather(w)
            _parent.weather = weather
        if _parent.sync:
            settings = _parent.world.get_settings()
            if not settings.synchronous_mode:
                self.synchronous_master = True
                _parent.traffic_manager.set_synchronous_mode(True)
                settings.synchronous_mode = True
                settings.fixed_delta_seconds = _parent.config.client_time_step
                _parent.world.apply_settings(settings)
            else:
                self.synchronous_master = False
                print('synchronous_master is False.')
        #
        print("Recording on file: %s" % _parent.client.start_recorder(self.filename))
        self.agent_list = []
        self.sensor_relation = {}
        self.sensor_thread = []
        HD_spawn_points, CAV_spawn_points = self.shuffle_spawn_points(_parent.map.initial_spawn_points, start=True)
        #
        self.HD_agents = self.spawn_actorlist('vehicle', _parent.HD_blueprints, HD_spawn_points)
        self.CAV_agents = self.spawn_actorlist('vehicle', _parent.CAV_blueprints, CAV_spawn_points)
        pass

    def stop_record(self):
        _parent = self.parent
        if _parent.sync:
            settings = _parent.world.get_settings()
            settings.synchronous_mode = False
            settings.fixed_delta_seconds = None
            _parent.world.apply_settings(settings)
        _parent.client.apply_batch([carla.command.DestroyActor(x) for x in self.CAV_agents+self.HD_agents])
        # self.client.apply_batch([carla.command.DestroyActor(x) for x in self.camera_list])
        print('\ndestroying %d vehicles' % len(self.CAV_agents+self.HD_agents))
        self.sensor_list = []
        for sensor in _parent.sensor_relation.values():
            self.sensor_list += sensor
        _parent.client.apply_batch([carla.command.DestroyActor(x) for x in self.sensor_list])
        print('\ndestroying %d sensors' % len(self.sensor_list))
        _parent.client.stop_recorder()
        print("Stop recording")
        pass

    pass

class ReplayWorker:
    def __init__(self, parent, filename, time_factor, start, duration, camera):
        self.parent = parent
        self.time_factor = time_factor
        self.filename = filename
        self.start, self.duration, self.camera = start, duration, camera
        pass

    @staticmethod
    def find_replay_time(output, duration):
        index_start = output.index('-') + 2
        index_end = output.index('(') - 2
        total_time = float(output[index_start:index_end])
        if duration == 0:
            return total_time
        else:
            return duration

    @staticmethod
    def get_road(world_map):
        def return_cor(waypoint):
            location = waypoint.transform.location
            return [location.x,location.y,location.z]

        WAYPOINT_DISTANCE = 10
        topology = world_map.get_topology()
        road_list = []
        for wp_pair in topology:
            current_wp = wp_pair[0]
            # Check if there is a road with no previus road, this can happen
            # in opendrive. Then just continue.
            if current_wp is None:
                continue
            # First waypoint on the road that goes from wp_pair[0] to wp_pair[1].
            current_road_id = current_wp.road_id
            wps_in_single_road = [return_cor(current_wp)]
            # While current_wp has the same road_id (has not arrived to next road).
            while current_wp.road_id == current_road_id:
                # Check for next waypoints in aprox distance.
                available_next_wps = current_wp.next(WAYPOINT_DISTANCE)
                # If there is next waypoint/s?
                if available_next_wps:
                    # We must take the first ([0]) element because next(dist) can
                    # return multiple waypoints in intersections.
                    current_wp = available_next_wps[0]
                    wps_in_single_road.append(return_cor(current_wp))
                else: # If there is no more waypoints we can stop searching for more.
                    break
            pcd1 = o3d.geometry.PointCloud()
            pdc2 = o3d.geometry.PointCloud()
            pcd1.points = o3d.utility.Vector3dVector(wps_in_single_road[:-1])
            pdc2.points = o3d.utility.Vector3dVector(wps_in_single_road[1:])
            corr = [(i,i+1) for i in range(len(wps_in_single_road)-2)]
            lineset = o3d.geometry.LineSet.create_from_point_cloud_correspondences(pcd1,pdc2,corr)
            lineset.paint_uniform_color(np.array([0.5, 0.5, 0.5]))
            road_list.append(lineset)
        return road_list

    def start_replay(self):
        _parent = self.parent
        settings = _parent.world.get_settings()        
        settings.synchronous_mode = True        
        settings.fixed_delta_seconds = _parent.config.client_time_step
        _parent.world.apply_settings(settings)
        # set the time factor for the replayer
        _parent.client.set_replayer_time_factor(self.time_factor)
        # replay the session
        output = _parent.client.replay_file(
            self.filename, self.start, self.duration, self.camera)
        replay_time = self.find_replay_time(output, self.duration)
        print('start replay...{}'.format(str(output)))
        return replay_time

    def stop_replay(self):
        _parent = self.parent
        actor_list =[]
        for actor in _parent.world.get_actors().filter('vehicle.*'):
            actor_list.append(actor.id)
        _parent.client.apply_batch([carla.command.DestroyActor(x) for x in actor_list])
        if _parent.sync: # and synchronous_master:
            settings = _parent.world.get_settings()
            settings.synchronous_mode = False
            settings.fixed_delta_seconds = None
            _parent.world.apply_settings(settings)
        print('destroying %d vehicles' % len(actor_list))
        _parent.world = _parent.client.reload_world()
        _parent.map.initial_spectator(_parent.config.map_spectator_point)
        exit()

    pass

class Map:
    def __init__(self, parent):
        _config = parent.config
        self.pretrain_model = True
        self.world = parent.world
        #
        self.initial_spectator(_config.map_spectator_point)
        self.tmp_spawn_points = self.world.get_map().get_spawn_points()
        if self.pretrain_model:
            self.initial_spawn_points =self.tmp_spawn_points
        else:
            self.initial_spawn_points = self.check_spawn_points(_config.map_initial_spawn_ROI)
        self.additional_spawn_points = self.check_spawn_points(_config.map_additional_spawn_ROI)
        self.destination = self.init_destination(self.tmp_spawn_points, _config.map_ROI)
        self.ROI = _config.map_ROI

    def initial_spectator(self, spectator_point):
        spectator = self.world.get_spectator()
        spectator_point_transform = carla.Transform(
            carla.Location( spectator_point[0][0],
                            spectator_point[0][1],
                            spectator_point[0][2]),
            carla.Rotation( spectator_point[1][0],
                            spectator_point[1][1],
                            spectator_point[1][2])
        )                                     
        spectator.set_transform(spectator_point_transform)

    def check_spawn_points(self, check_spawn_ROI):
        tmp_spawn_points = []
        tmp_x,tmp_y = [],[]
        for tmp_transform in self.tmp_spawn_points:
            tmp_location = tmp_transform.location
            for edge in check_spawn_ROI:
                if tmp_location.x > edge[0] and tmp_location.x < edge[1] and tmp_location.y > edge[2] and tmp_location.y < edge[3]:
                    tmp_spawn_points.append(tmp_transform)
                    tmp_x.append(tmp_location.x)
                    tmp_y.append(tmp_location.y)
                    continue
        # self.plot_points(tmp_x,tmp_y)
        return tmp_spawn_points
    
    def init_destination(self, spawn_points, ROI):
        destination = []
        tmp_x,tmp_y = [],[]
        for p in spawn_points:
            if not self.inROI([p.location.x, p.location.y], ROI):
                destination.append(p)
                tmp_x.append(p.location.x)
                tmp_y.append(p.location.y)
        # self.plot_points(tmp_x,tmp_y)
        return destination
    
    @staticmethod
    def inROI(x,ROI):
        def sign(a,b,c):
            return (a[0]-c[0]) * (b[1]-c[1]) - (b[0]-c[0]) * (a[1]-c[1])
        
        d1=sign(x,ROI[0],ROI[1])
        d2=sign(x,ROI[1],ROI[2])
        d3=sign(x,ROI[2],ROI[3])
        d4=sign(x,ROI[3],ROI[0])

        has_neg=(d1<0) or (d2<0) or (d3<0) or (d4<0)
        has_pos=(d1>0) or (d2>0) or (d3>0) or (d4>0)
        return not(has_neg and has_pos)

    @staticmethod
    def plot_points(tmp_x,tmp_y):
        plt.figure(figsize=(8,7))
        ax = plt.subplot(111)
        ax.axis([-50,250,50,350])
        ax.scatter(tmp_x,tmp_y)
        for index in range(len(tmp_x)):
            ax.text(tmp_x[index],tmp_y[index],index)
        plt.show()

    pass

class ScenarioManager:
    def __init__(self):
        self.init_config()
        self.init_calibration()
        self.init_connection()
        self.recording = False
        pass

    def init_config(self):
        self.config = Configuration('CARLA')
        self.sync = self.config.client_sync
        try:
            self.config.map_name = TOWN_MAP #override default value
        except:
            pass
        pass

    def init_calibration(self):
        self.calibration = np.identity(3)
        self.calibration[0, 2] = self.config.calibration_image_width  / 2.0
        self.calibration[1, 2] = self.config.calibration_image_height / 2.0
        self.calibration[0, 0] = self.config.calibration_image_width / (
            2.0 * np.tan( self.config.calibration_VIEW_FOV * np.pi / 360.0 )
        )
        self.calibration[1, 1] = self.calibration[0, 0]
        pass

    def init_connection(self):
        self.client = carla.Client(self.config.client_host, self.config.client_port)
        self.client.set_timeout(self.config.client_timeout)
        self.world = self.client.load_world(self.config.map_name)
        self.traffic_manager = self.client.get_trafficmanager(self.config.client_tm_port)
        self.map = Map(self)
        # agent information
        self.HD_blueprints  = self.world.get_blueprint_library().filter('vehicle.*')
        self.CAV_blueprints = self.world.get_blueprint_library().filter('vehicle.tesla.*')
        # sensor information
        self.sensor_attribute = [
            ['sensor.camera.rgb', cc.Raw, 'Camera RGB', {}],
            ['sensor.lidar.ray_cast', None, 'Lidar (Ray-Cast)', {}]
        ]
        self.sensor_transform = [
            (carla.Transform(carla.Location(x=0, z=2.5)), Attachment.Rigid),
            (carla.Transform(carla.Location(x=0, z=2.5)), Attachment.Rigid)
        ]
        pass

    def start_world_tick(self, no_record=False):
        weak_ref = weakref.ref(self)
        if no_record:
            self.world.on_tick(lambda _: None)
        else:
            self.world.on_tick(lambda _snapshot: self.on_world_tick(weak_ref, _snapshot))
        pass

    def run_step(self):
        if not self.map.pretrain_model:
            self.check_vehicle_state()
        return self.world.tick()

    def check_vehicle_state(self):
        for v_id in self.HD_agents:
            vehicle = self.world.get_actor(v_id)
            v_position = vehicle.get_transform().location
            if not(self.map.inROI([v_position.x, v_position.y], self.map.ROI)):
                vehicle.destroy()
                self.HD_agents.remove(v_id)
        #
        for v_id in self.CAV_agents:
            vehicle = self.world.get_actor(v_id)
            v_position = vehicle.get_transform().location
            if not(self.map.inROI([v_position.x, v_position.y], self.map.ROI)):
                for sensor_id in self.sensor_relation[str(v_id)]:
                    sensor = self.world.get_actor(sensor_id)
                    if sensor.is_listening:
                        print(sensor.id)
                        sensor.stop()
                    sensor.destroy()
                #
                self.sensor_relation.pop(str(v_id))
                vehicle.destroy()
                self.CAV_agents.remove(v_id)
            pass
        pass

    @staticmethod
    def on_world_tick(weak_ref, _snapshot):

        def parse_transform(transform):
            return [transform.location.x,transform.location.y,transform.location.z,transform.rotation.roll,transform.rotation.pitch,transform.rotation.yaw]
        
        def parse_bounding_box(bounding_box):
            return [bounding_box.extent.x,bounding_box.extent.y,bounding_box.extent.z, bounding_box.location.z]

        _self = weak_ref()
        if _snapshot.frame % _self.config.client_sample_frequency != 0:
            return
        #
        actors = _self.world.get_actors()
        vehicles, sensors, CAV_vehicles = [], [], []
        for actor in actors:
            str_actor = [str(actor.type_id),actor.id] + parse_transform(actor.get_transform())
            # if 'lidar' in actor.type_id or 'rgb' in actor.type_id:
            #     print(str(actor.type_id),actor.get_transform().rotation.pitch,actor.get_transform().rotation.roll)
            if 'vehicle' in actor.type_id:
                str_actor += parse_bounding_box(actor.bounding_box)
                vehicles.append(str_actor)
            elif 'sensor' in actor.type_id:
                str_actor += [0,0,0] + [actor.parent.id]
                sensors.append(str_actor)
            pass
        actors = np.array(vehicles + sensors)
        #
        if _self.recording:
            _label_path = Path(_self.raw_data_path, 'label')
            _label_path.mkdir(parents=True, exist_ok=True)
            if len(actors) != 0:
                _filename = ( _label_path / ('%010d.txt'%(_snapshot.frame)) ).as_posix()
                np.savetxt(_filename, actors, fmt='%s', delimiter=' ')
        
        # 2D bounding box
        vehicles = _self.world.get_actors().filter('vehicle.*')
        for vehicle in vehicles:
            if str(vehicle.id) in _self.sensor_relation.keys():
                sensor_list = _self.sensor_relation[str(vehicle.id)]
            else:
                continue
            calib_info = []
            for sensor_id in sensor_list:
                sensor = _self.world.get_actor(sensor_id)
                if 'lidar' in sensor.type_id:
                    lidar = _self.world.get_actor(sensor_id)
            #
            for sensor_id in sensor_list:
                sensor = _self.world.get_actor(sensor_id)
                if 'rgb' in sensor.type_id:
                    sensor.calibration = _self.calibration
                    tmp_bboxes = ClientSideBoundingBoxes.get_bounding_boxes(vehicles,sensor)
                    if _self.recording:
                        image_label_path = Path(_self.raw_data_path,
                                                vehicle.type_id + '_' + str(vehicle.id),
                                                sensor.type_id + '_' + str(sensor.id)
                                            ).as_posix()
                        if not os.path.exists(image_label_path+'_label'):
                            os.makedirs(image_label_path+'_label')
                        if len(tmp_bboxes) != 0:
                            np.savetxt(image_label_path+'_label/%010d.txt' % _snapshot.frame, tmp_bboxes, fmt='%s', delimiter=' ')
            pass
        pass

    #=================================================================================#
    def look_for_spawn_points(self):
        worker = SpawnWorker(self,
            self.config.client_time_step, self.sync)
        self.start_world_tick()
        #
        try:
            worker.start_look()
            if not self.sync or not worker.synchronous_master:
                self.world.wait_for_tick()
            else:
                start = self.world.tick()
            #
            while True:
                if self.sync and worker.synchronous_master:
                    now = self.run_step()
                    if (now - start) % 1000 == 0:
                        print('Frame ID:'+str(now))
                else:
                    self.world.wait_for_tick()
        finally:
            # print('stop from frameID: %s.' % now)
            worker.stop_look()
        pass

    def generate_data(self, hd:list, av:list):
        _id = time.strftime( '%Y-%m%d-%H%M', time.localtime() )
        self.raw_data_path = RAW_DATA_PATH / ('record'+_id)
        _record_filename = (LOG_PATH / 'record-%s.log'%_id).as_posix()
        self.recording = True
        #
        worker = RecordWorker(self, _record_filename, hd, av)
        self.start_world_tick()
        #
        try:
            self.start_record()
            if not self.sync or not worker.synchronous_master:
                self.world.wait_for_tick()
            else:
                start = self.world.tick()  
                if self.dynamic_weather:
                    self.weather.tick(1)
                    self.world.set_weather(self.weather.weather)
                print('start from frameID: %s.' % start)
            #
            while True:
                if self.sync and worker.synchronous_master:
                    time.sleep(1)
                    now = self.run_step()
                    if (now - start) % 1000 == 0:
                        print('Frame ID:'+str(now))
                else:
                    self.world.wait_for_tick()
        finally:
            try:
                print('stop from frameID: %s.' % now)
            finally:
                pass
            self.stop_record()
            pass
        pass

    def find_and_replay(self, filename:str, rate:float, start:int, duration:int, camera:int):
        worker = ReplayWorker(self, filename, rate, start, duration, camera)
        self.recording = True
        # road_net = worker.get_road(self.world.get_map())
        # mesh = o3d.geometry.TriangleMesh.create_coordinate_frame(size=100)
        # o3d.visualization.draw_geometries(road_net+[mesh],height=1280,width=1920)
        self.start_world_tick(no_record=True)
        #
        try:
            start_time = time.time()
            replay_time = worker.start_replay()
            while time.time() - start_time < replay_time:
                self.world.tick()
        finally:
            print('stop replay...')
            time.sleep(2)
            self.stop_replay()
        pass
    pass

def execute(sm:ScenarioManager, command:str, args):
    if command=='spawn':
        sm.look_for_spawn_points()
    elif command=='record':
        args.hd.strip('()[]<>'); args.av.strip('()[]<>')
        _hd = [ int(_id) for _id in args.hd.split(',') ]
        _av = [ int(_id) for _id in args.av.split(',') ]
        sm.generate_data(_hd, _av)
    elif command=='replay':
        sm.find_and_replay(args.filename,
            args.rate, args.start, args.duration, args.camera)
    else:
        print('The command <{}> is not supported.'.format(command))
    pass

def init_subparsers(subparsers):
    parser_spawn = subparsers.add_parser('spawn', help='spawn points on map.')
    # parser_spawn.add_argument('--sync', action='store_true')
    #
    parser_record = subparsers.add_parser('record', help='record a trace.')
    parser_record.add_argument('--hd', help='list of human-driving vehicles. (split by ",")')
    parser_record.add_argument('--av', help='list of autonomous vehicles. (split by ",")')
    #
    parser_replay = subparsers.add_parser('replay', help='replace a trace.')
    parser_replay.add_argument('filename', nargs=1, help='path to record file.')
    parser_replay.add_argument('--rate', nargs='?', type=float, default=1.0,
        help='replayer: time factor.')
    parser_replay.add_argument('--start', nargs='?', type=int, default=0,
        help='replayer: start')
    parser_replay.add_argument('--duration', nargs='?', type=int, default=0,
        help='replayer: duration')
    parser_replay.add_argument('--camera', nargs='?', type=int, default=0,
        help='replayer: camera')
    pass

def main():
    parser = argparse.ArgumentParser(
        description='CARLA Raw Dataset Record and Replay'
    )
    subparsers = parser.add_subparsers(dest='command')
    init_subparsers(subparsers)
    args = parser.parse_args()
    #
    sm = ScenarioManager()
    execute(sm, args.command, args)
    pass

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        raise e if DBG else ''
    finally:
        '' if DBG else exit()
