#!/usr/bin/python

import time
import io, json 					#needed for exporting payloads to json file
import paho.mqtt.client as mqtt

import argparse
import logging
import base64
import binascii
import httplib # be careful for python3!


class mqttStoreForward:

	#class variables
	isConnected = False
	isOnMessage = False
				#checks ethernet connection
	lora_client = mqtt.Client()
	packet = None 						#will carry msg payload...empty to begin
	isJsonEmpty = True 					#keep track of whether file is empty
	jsonFilePath = 'packetStorage.json' #INPUT DESIRED JSON FILE NAME HERE OR LEAVE DEFAULT
	payloadData = None
	devEUI = None
	rbAuthorization = ''

	def __init__ (self):
		#touch jston file if it does not exist
		file = open(self.jsonFilePath, 'a')
		file.close()

	#connect lora client to localhost
	def setLoraClient(self):
		self.lora_client.connect("127.0.0.1")

	#callback function initiated on on_connect property for lora client
	def loraOnConnect(self, client, userdata, flags, rc):
		print("Lora Client Connection: " + str(rc)) 	#Returns a 0
		self.lora_client.subscribe("lora/+/up", qos=0)
		self.isConnected = True

	#callback function initiated on on_disconnect property for both clients
	def onDisconnect(self, client, userdata, rc):
		self.isConnected = False
		print("The connection has failed.")

## formats the paayload message from the endpoint
	def rbPayloadFormatters(self, msg):
		msgObj = json.loads(msg)
		newMsg = {}
		msgHex = base64.b64decode(msgObj["data"])
		newMsg["payload"] = binascii.hexlify(msgHex)
		newMsg["time"] = msgObj["tmst"]
		newMsg["snr"] = msgObj["lsnr"]
		newMsg["station"] = msgObj["appeui"]
		newMsg["avgsnr"] = msgObj["lsnr"]
		newMsg["lat"] = 0
		newMsg["lng"] = 0
		newMsg["rssi"] = msgObj["rssi"]
		newMsg["seqnumber"] = msgObj["seqn"]
		newMsg["deveui"] = msgObj["deveui"]
		newMsg["authorisation"] = self.rbAuthorization
		return json.dumps(newMsg)


	#call back function initiated on on_message
	def onMessage(self, mqtt_client, userdata, msg):
		self.packet = self.rbPayloadFormatters(msg.payload)
		pkt = json.loads(self.packet)
		self.devEUI = pkt["deveui"]
		self.payloadData = pkt["payload"]
		self.isOnMessage = True


		print(self.packet)
		#### HTTP REQUEST GOES HERE ####
		rbConnection = httplib.HTTPSConnection("console.radiobridge.com")
		rbHeaders = {"Content-Type": "application/json", "Accept":"application/json"}
		rbConnection.request("POST", "/uplink_api_callback", self.packet, rbHeaders)
		rbResponse = rbConnection.getresponse()
		rbResponseMsg = rbResponse.read()

		if(rbResponseMsg != ""):
			try:
				##formats the downlink message##
				rbResponseMsg = json.loads(rbResponseMsg)
				newMsg = {}
				msg64 = binascii.unhexlify(rbResponseMsg["payload"])
				newMsg["data"] = binascii.b2a_base64(msg64)
				newMsg["ack"] = "false"
				newMsg["port"] = 1
				newMsg = json.dumps(newMsg)
				print("Radiobridge reply was " + str(rbResponseMsg) + " downlink was 					published")

				self.lora_client.publish("lora/"+ self.devEUI + "/down", newMsg)


			except:
				print('Radiobridge reply was ' + str(rbResponseMsg) + ' downlink did not 					publish')



   	#set callback properties of both clients to their respective functions(from above)
	def setVals(self):
		self.lora_client.on_connect = self.loraOnConnect
		self.lora_client.on_message = self.onMessage
		self.lora_client.on_disconnect = self.onDisconnect


			#takes packet parameter and appends it to a file
	def writeToJson(self, data):
		with open(self.jsonFilePath, 'a') as myFile:
			myFile.write(data + "\r\n")

 	#Controls what is done with the packet depending on a working/not working connection
	def checkConnect(self, packet):
		if(self.isConnected == True):
			#check whether the file is empty/has stored packets every time the
			#connection is found to be good
			self.checkJsonFile()
			##################################################################################
			#ADD YOUR CODE HERE. WHEN THE CONNECTION IS WOKRING, DECIDE WHAT TO DO WITH PACKET
			##################################################################################
			print("PRINTING PACKET/CONNECTION OK")
			print("PRINTED: " + packet)

		else:
			#When the connection is bad, we write the packet to the json file for storage.
			print("ADDING TO JSON FILE. CONNECTION DOWN") #Ethernet down
			'''FOR TESTING, make sure that this packet matches one printed when it reconnects
			 and forwards packets from json storage: '''
			print("STORED: " + packet)
			self.writeToJson(packet) #store
	#Creates infinite loop needed for paho mqtt loop_forever()
	def runLoop(self):
		while(True):
			time.sleep(1)

	#Creates event loop and new thread that initializes the paho mqtt loops for both clients
	def startLoop(self):
		#UI thread = terminal interaction
		self.lora_client.loop_start()

	'''Check the json file to see if it's empty. If it is, do nothing, if there are stored
	packets, forward them, line by line'''
	def checkJsonFile(self):
		with open(self.jsonFilePath, 'r') as output:
			output.seek(0) #go to beginning of file
			charTrue = output.read(1) #retrieve first character of json file, if it exists
			if not charTrue:
				print("File is empty. Nothing to forward.")
			else:
				#file is not empty and we have data to forward
				print("FORWARDING DATA!!!!")
				with open(self.jsonFilePath) as forwardFile:
					linesGroup = forwardFile.read().splitlines()
					forwardFile.close()
					#############################################################################
					'''ADD YOUR CODE HERE! WHEN THERE ARE PACKETS STORED IN THE JSON FILE, DECIDE
					 WHAT TO DO WITH THEM WHEN THE CONNECTION IS BACK UP AGAIN'''
					#############################################################################
					for line in linesGroup:
						print("FORWARDING STORED PACKET")
						#publish the data to the server topic!!!

						print(line)

				#After all data is forwarded, CLEAR THE FILE!!
				with open(self.jsonFilePath, 'r+') as clearFile:
					clearFile.truncate(0)

def main():
	instance = mqttStoreForward() 	#create instance of class
	instance.startLoop()
	#need to call setVals first because they wont connect to the call backs if the setClient functions are called first
	instance.setVals() 				#set connect and message properties & infinite loop
	instance.setLoraClient()
		#connect to local host
	instance.runLoop()

if __name__ == "__main__":
	main()
