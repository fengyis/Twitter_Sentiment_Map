<!DOCTYPE html>
<html>
	<head>
		<meta charset="utf-8">
		<title>Heatmaps</title>
		<style>
			html, body, #map-canvas {
				height: 100%;
				margin: 0px;
				padding: 0px
			}
			#panel {
				position: absolute;
				top: 5px;
				left: 35%;
				margin-left: -180px;
				z-index: 5;
				background-color: #ff9;
				padding: 5px;
				border: 1px solid #999;
			}
		</style>
		<script src="https://maps.googleapis.com/maps/api/js?v=3.exp&signed_in=true&libraries=visualization"></script>
		<script>
			var map, pointarray, heatmap, pointarray2, heatmap2;

			var tweetData = [];
			var tweetData2 = [];
			var tweetData3 = [];

			var wsUri = "ws://localhost:8080/Twitt-Map/echo";

			function initialize() {

				var mapOptions = {
					zoom: 2,
					center: new google.maps.LatLng(36.673546, -121.143536)
				};

				map = new google.maps.Map(document.getElementById('map-canvas'),
						mapOptions);

				var pointArray = new google.maps.MVCArray(tweetData);
				var pointArray2 = new google.maps.MVCArray(tweetData2);
				var pointArray3 = new google.maps.MVCArray(tweetData3);

				heatmap = new google.maps.visualization.HeatmapLayer({
					data: pointArray
				});
				
				heatmap2 = new google.maps.visualization.HeatmapLayer({
						data: pointArray2
					});
				heatmap3 = new google.maps.visualization.HeatmapLayer({
					data: pointArray3
				});
				
				heatmap.setMap(map);
				heatmap2.setMap(map);
				heatmap3.setMap(map);
				
			}

			setInterval(function(){ update() }, 1000);
				function update() {
					var pointArray = new google.maps.MVCArray(tweetData);
					heatmap.set('data', pointArray);
					var pointArray2 = new google.maps.MVCArray(tweetData2);
					heatmap2.set('data', pointArray2);
					var pointArray3 = new google.maps.MVCArray(tweetData3);
					heatmap3.set('data', pointArray3);
				} 

			function toggleHeatmap() {
				heatmap.setMap(heatmap.getMap() ? null : map);
			}

			function changeGradient() {
				var gradient = [
					'rgba(0, 255, 255, 0)',
					'rgba(0, 255, 255, 1)',
					'rgba(0, 191, 255, 1)',
					'rgba(0, 127, 255, 1)',
					'rgba(0, 63, 255, 1)',
					'rgba(0, 0, 255, 1)',
					'rgba(0, 0, 223, 1)',
					'rgba(0, 0, 191, 1)',
					'rgba(0, 0, 159, 1)',
					'rgba(0, 0, 127, 1)',
					'rgba(63, 0, 91, 1)',
					'rgba(127, 0, 63, 1)',
					'rgba(191, 0, 31, 1)',
					'rgba(255, 0, 0, 1)'
				]
				heatmap2.set('gradient', heatmap2.get('gradient') ? null : gradient);
				
				var gradient1 = [
					'rgba(0, 0, 255, 0)',
					'rgba(0, 0, 255, 1)',
					'rgba(0, 0, 191, 1)',
					'rgba(0, 45, 123, 1)',
					'rgba(0, 63, 255, 1)',
					'rgba(0, 0, 0, 1)',
					'rgba(0, 0, 5, 1)',
					'rgba(0, 0, 14, 1)',
					'rgba(0, 0, 54, 1)',
					'rgba(0, 0, 23, 1)',
					'rgba(63, 0, 67, 1)',
					'rgba(127, 0, 1, 1)',
					'rgba(191, 0, 0, 1)',
					'rgba(255, 0, 4, 1)'				                 
				]
				heatmap3.set('gradient', heatmap2.get('gradient') ? null : gradient1);
			}

			function changeRadius() {
				heatmap.set('radius', heatmap.get('radius') ? null : 20);
			}

			function changeOpacity() {
				heatmap.set('opacity', heatmap.get('opacity') ? null : 0.2);
			}

			function clearMap() {
					tweetData = [];
					var pointArray = new google.maps.MVCArray(tweetData);
					heatmap.set('data', pointArray);
			}

			function useHistoricData() {
					tweetData = [];
					var pointArray = new google.maps.MVCArray(tweetData);
					heatmap.set('data', pointArray);
					
					var inputString = document.getElementById("filter").value;
					
					websocket = new WebSocket(wsUri);
					websocket.onopen = function() {
							websocket.send("USEHISTORICDATA "+ inputString);
					};
					websocket.onmessage = function(evt) {
							var message = evt.data.split(" ", 2);
							var latitude = parseFloat(message[0]);
							var longitude = parseFloat(message[1]);
							tweetData.push(new google.maps.LatLng(parseFloat(message[0]), parseFloat(message[1])));
					};
					websocket.onerror = function(evt) {
					};
			}

			function useRealTimeData() {
					tweetData = [];
					var pointArray = new google.maps.MVCArray(tweetData);
					heatmap.set('data', pointArray);
					
					var inputString = document.getElementById("filter").value;
					
					websocket = new WebSocket(wsUri);
					websocket.onopen = function() {
							websocket.send("USEREALTIMEDATA "+ inputString);
					};
					websocket.onmessage = function(evt) {
							var message = evt.data.split(" ", 2);
							var latitude = parseFloat(message[0]);
							var longitude = parseFloat(message[1]);
							tweetData.push(new google.maps.LatLng(parseFloat(message[0]), parseFloat(message[1])));
					};
					websocket.onerror = function(evt) {
					};
					websocket.onclose = function(evt) {
					};
					
					
			}
			
			function sentiment() {
					tweetData = [];
					tweetData2 = [];
					tweetData3 = [];
					var pointArray = new google.maps.MVCArray(tweetData);
					var pointArray2 = new google.maps.MVCArray(tweetData2);
					var pointArray3 = new google.maps.MVCArray(tweetData3);
					heatmap.set('data', pointArray);
					heatmap2.set('data', pointArray2);
					heatmap3.set('data', pointArray3);
					
					var inputString = document.getElementById("filter").value;
					
					websocket = new WebSocket(wsUri);
					websocket.onopen = function() {
							websocket.send("SENTIMENT "+ inputString);
					};
					websocket.onmessage = function(evt) {
							var message = evt.data.split(" ", 3);
							var latitude = parseFloat(message[0]);
							var longitude = parseFloat(message[1]);
							var sentiment = parseInt(message[2]);
							
							if (sentiment > 0) {
//	window.alert(1);
								tweetData.push(new google.maps.LatLng(parseFloat(message[0]), parseFloat(message[1])));
//              }
							} else if (sentiment < 0) {
								tweetData2.push(new google.maps.LatLng(parseFloat(message[0]), parseFloat(message[1])));
						 } else if (sentiment ==0){
							 tweetData3.push(new google.maps.LatLng(parseFloat(message[0]), parseFloat(message[1])));
						 }
							
							
					};
					websocket.onerror = function(evt) {
					};
					websocket.onclose = function(evt) {
					};				
			}

			function closeConnection() {
				websocket.close();
			}

			google.maps.event.addDomListener(window, 'load', initialize);
		</script>
	</head>

	<body>
		<div id="panel" align="middle">
			<p>Assignment 2 (Junchao Zhang/Fengyi Song)</p>
			<p><small>Enter a filter word or leave it empty. If you want to switch between Sentiment and Historical Twitters, you should click"close connection" first</small></p>
			<p>
			<button onclick="useHistoricData()">Show DataBase Data</button>
			<button onclick="closeConnection()">Close Connection</button>
			<button onclick="sentiment()">Sentiment</button>
			<i>Filter:</i>
			<input id="filter words" type="text">
			</p>
			<p>
			<button onclick="clearMap()">Clear map</button>
			<button onclick="changeRadius()">Change radius</button>
			<button onclick="changeOpacity()">Change opacity</button>
			<button onclick="toggleHeatmap()">Toggle heatmap</button>
			<button onclick="changeGradient()">Change gradient</button>
			</p>
		</div>
		<div id="map-canvas"></div>
	</body>
</html>