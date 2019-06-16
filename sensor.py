import logging
import voluptuous
from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.helpers import config_validation
from homeassistant.helpers.entity import Entity

from threading import Lock
from urllib.request import urlopen
from xml.sax.handler import ContentHandler
from xml.sax import make_parser
import struct
import datetime
import time
import calendar
import re
import bson

DOMAIN = "tv_program"
CACHE_FILE = "./test.bson"
CACHE_EXPIRACY = 2 * 24 * 60; #2 days cache file
_LOGGER = logging.getLogger(__name__)

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend({
    voluptuous.Required("provider"):config_validation.string,
    voluptuous.Required("channels"):
        voluptuous.All(config_validation.ensure_list),
    voluptuous.Optional("timetowatch", default=["now"]):
        voluptuous.All(config_validation.ensure_list)
})

class ProgramItem:
    def parseDate(dateStr):
        if dateStr == None:
            return None
        date = datetime.datetime.fromtimestamp(calendar.timegm(time.strptime(dateStr[0:12], "%Y%m%d%H%M")))
        pos = re.search("[+-]", dateStr)
        if pos == None:
            return date
        pos = pos.start()
        offset = datetime.timedelta(
                hours=int(dateStr[pos +1:pos +3]),
                minutes=int(dateStr[pos +3:pos +5]))
        if (dateStr[pos] != '-'):
            offset *= -1
        date += offset
        return date.timestamp()

    def __init__(self, channelId, start, end):
        self.channelId = channelId
        self.start = ProgramItem.parseDate(start)
        self.end = ProgramItem.parseDate(end)
        self.title = None
        self.description = None
        self.category = None

    def pack(self):
        return {
            "start": int(self.start /60),
            "end": int(self.end /60),
            "title": self.title,
            "description": self.description,
            "category": self.category
        }

    def unpack(channelId, data):
        result = ProgramItem(channelId, None, None)
        result.start = data["start"] *60
        result.end = data["end"] *60
        result.title = data["title"]
        result.description = data["description"]
        result.category = data["category"]
        return result

def strToTimeDelta(str):
    found = re.findall("([0-9]+)h([0-9]{0,2})", str)
    if found and found[0] and found[0][0]:
        hours = int(found[0][0])
        minutes = 0
        if found[0][1] != '':
            minutes = int(found[0][1])
        return datetime.timedelta(hours=hours, minutes=minutes)

class Channel:
    def __init__(self, id):
        self.id = id
        self.name = None
        self.icon = None
        self.program = list()

    def getProgramForTime(self, ts):
        for i in self.program:
            if i.end > ts:
                return i

    def pack(self):
        program = list()
        for i in self.program:
            program.append(i.pack())
        return {
            "id": self.id,
            "name": self.name,
            "icon": self.icon,
            "program": program
        }

    def unpack(data):
        result = Channel(data["id"])
        result.name = data["name"]
        result.icon = data["icon"]
        for i in data["program"]:
            program = ProgramItem.unpack(data["id"], i)
            if i:
                result.program.append(program)
        return result

class ProgramContentHandler(ContentHandler):
    def __init__(self):
        self.currentChannel = None
        self.currentProgram = None
        self.currentString = None
        self.channels = dict()

    def startElement(self, name, attrs):
        if (name == "channel" and attrs.getValue("id") != None):
            self.currentChannel = Channel(attrs.getValue("id").strip())
        elif (name == "display-name" and self.currentChannel != None):
            self.currentString = ''
        elif ((name == "title" or name == "desc" or name == "category") and self.currentProgram != None):
            self.currentString = ''
        elif (name == "icon" and attrs.getValue("src") != None and self.currentChannel != None):
            self.currentChannel.icon = attrs.getValue("src").strip()
        elif (name == "programme" and attrs.getValue("start") != None and attrs.getValue("stop") != None and attrs.getValue("channel") != None):
            self.currentProgram = ProgramItem(attrs.getValue("channel").strip(), attrs.getValue("start").strip(), attrs.getValue("stop").strip())

    def characters(self, content):
        if (self.currentString != None):
            self.currentString += content

    def endElement(self, name):
        if (name == "display-name" and self.currentString != None and self.currentChannel != None):
            if (self.currentChannel.name == None):
                self.currentChannel.name = self.currentString.strip()
            self.currentString = None
        elif (name == "channel" and self.currentChannel != None):
            if (self.currentChannel.name != None):
                self.channels[self.currentChannel.id] = self.currentChannel
            self.currentChannel = None
        elif (name == "title" and self.currentProgram != None):
            if (self.currentProgram.title == None):
                self.currentProgram.title = self.currentString
            self.currentString = None
        elif (name == "desc" and self.currentProgram != None):
            if (self.currentProgram.description == None):
                self.currentProgram.description = self.currentString
            self.currentString = None
        elif (name == "category" and self.currentProgram != None):
            if (self.currentProgram.category == None):
                self.currentProgram.category = self.currentString
            self.currentString = None
        elif (name == "programme" and self.currentProgram != None):
            channel = self.channels[self.currentProgram.channelId]
            if (channel != None and self.currentProgram.title != None):
                channel.program.append(self.currentProgram)
            self.currentProgram = None

class ProgramBuilder:
    def readCache(channelId):
        fd = 0
        try:
            fd = open(CACHE_FILE, "rb")
        except FileNotFoundError:
            _LOGGER.info("Cache file not found")
            return None
        try:
            cacheTime = struct.unpack('L', fd.read(struct.calcsize('L')))[0]
            if cacheTime +CACHE_EXPIRACY < time.time() /60:
                _LOGGER.info("Cache file expired")
                fd.close()
                return None
            while fd.readable():
                length = struct.unpack('Q', fd.read(struct.calcsize('Q')))[0]
                data = bson.loads(fd.read(length))
                if data["id"] == channelId:
                    fd.close()
                    return Channel.unpack(data)
        except struct.error:
            _LOGGER.error("Corrupted data")
            fd.close()
            return None
        # Channel not found
        fd.close()
        return None

    def writeCache(data):
        fd = open(CACHE_FILE, "wb")
        fd.write(struct.pack('L', *[int(time.time() / 60)]))
        for i in data:
            channelData = bson.dumps(data[i].pack())
            fd.write(struct.pack('Q', *[len(channelData)]))
            fd.write(channelData)
        fd.close()

    def fromProvider(provider):
        _LOGGER.info("Fetching " +provider)
        parser = make_parser()
        contentHandler = ProgramContentHandler()
        parser.setContentHandler(contentHandler)
        parser.parse(urlopen(provider))
        return contentHandler.channels

    def createProgram(defaultProvider, channelId):
        ProgramBuilder.mutex.acquire()
        result = ProgramBuilder.readCache(channelId)
        if result == None:
            result = ProgramBuilder.fromProvider(defaultProvider)
            ProgramBuilder.writeCache(result)
            result = result[channelId]
        ProgramBuilder.mutex.release()
        return result

ProgramBuilder.mutex = Lock()

class ProgramSensor(Entity):
    def channelIdToEntity(channelId, time):
        return "tv_program." +re.sub(r'\W+', '', channelId) +'_' +time

    def __init__(self, module, channelId, time):
        self.module = module
        self.entityId = ProgramSensor.channelIdToEntity(channelId, time)
        self.channelId = channelId
        self.time = time
        self.currentValue = None

    def getRequestTime(self):
        requestedTs = datetime.datetime.now();
        requestedTime = self.time
        if requestedTime == "now":
            return requestedTs.timestamp()
        requestedTs = requestedTs.replace(hour=0, minute=0, second=0, microsecond=0)
        if requestedTime == "tonight":
            requestedTime = "21h30"
        requestedTs += strToTimeDelta(requestedTime)
        return requestedTs.timestamp()

    @property
    def device_state_attributes(self):
        if self.currentValue:
            return {
                "title": self.currentValue.title,
                "description": self.currentValue.description,
                "category": self.currentValue.category,
                "channelName": self.currentChannel.name,
                "channelIcon": self.currentChannel.icon,
                "start": datetime.datetime.fromtimestamp(self.currentValue.start),
                "end": datetime.datetime.fromtimestamp(self.currentValue.end),
                "duration": int((self.currentValue.end -self.currentValue.start) /60)
            }


    @property
    def name(self):
        return self.channelId +"." +self.time

    @property
    def state(self):
        if self.currentValue:
            return self.currentValue.title
        return "Unknown"

    def update(self):
        self.currentChannel = ProgramBuilder.createProgram(self.module.provider, self.channelId)
        if self.currentChannel != None:
            self.currentValue = self.currentChannel.getProgramForTime(self.getRequestTime())
        else:
            self.currentValue = None

class TvProgramModule:
    def __init__(self):
        self.provider = None
        self.channelToWatch = list()
        self.timeToWatch = list()

    def setChannels(self, channels):
        if channels:
            self.channelToWatch = channels
        else:
            self.channelToWatch = list()

    def setTimes(self, times):
        if times:
            self.timeToWatch = times
        else:
            self.timeToWatch = list()

    def createSensors(self, hass):
        sensors = []
        for i in self.channelToWatch:
            for time in self.timeToWatch:
                sensors.append(ProgramSensor(self, i, time))
        return sensors

TV_PROGRAM_INSTANCE = TvProgramModule()

def setup_platform(hass, config, addSensors, discoveryInfo=None):
    TV_PROGRAM_INSTANCE.provider = config["provider"]
    TV_PROGRAM_INSTANCE.setChannels(config["channels"])
    TV_PROGRAM_INSTANCE.setTimes(config["timetowatch"])
    sensors = TV_PROGRAM_INSTANCE.createSensors(hass)
    addSensors(sensors, True)
    return True

