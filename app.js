const { DeckGL, ScatterplotLayer, ArcLayer, LinearInterpolator } = deck;

let nodes = [];
let traffic = [];
let currentTime = 0;
let isPaused = false;
let lastTimestamp = 0;

const slider = document.getElementById('slider');
const playBtn = document.getElementById('play-btn');
const timeDisplay = document.getElementById('time-display');

const deckgl = new DeckGL({
  container: 'container',
  mapStyle: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  initialViewState: {
    longitude: 10,
    latitude: 40,
    zoom: 2,
    pitch: 45,
    bearing: 0
  },
  controller: true,
  layers: []
});

async function loadData() {
  const nodesResponse = await fetch('nodes.json');
  nodes = await nodesResponse.json();
  
  const trafficResponse = await fetch('traffic.json');
  traffic = await trafficResponse.json();
  
  // Normalize traffic times to a 12s window for visualization
  // Find the first event time
  if (traffic.length > 0) {
      const minTime = Math.min(...traffic.map(t => t.start_time_ms));
      traffic.forEach(t => {
          t.start_time_ms -= minTime;
          t.arrival_time_ms -= minTime;
          // Loop within 12s
          t.start_time_ms %= 12000;
          t.arrival_time_ms %= 12000;
          if (t.arrival_time_ms < t.start_time_ms) t.arrival_time_ms += 12000;
      });
  }
  
  render();
}

function render() {
  const nodeMap = new Map(nodes.map(n => [n.id, n]));

  const activeTraffic = traffic.filter(t => 
      currentTime >= t.start_time_ms && currentTime <= t.arrival_time_ms
  );

  const layers = [
    new ScatterplotLayer({
      id: 'nodes',
      data: nodes,
      getPosition: d => [d.lon, d.lat],
      getFillColor: d => d.color,
      getRadius: 10,
      radiusMinPixels: 5,
      pickable: true
    }),
    new ArcLayer({
        id: 'traffic-arcs',
        data: activeTraffic,
        getSourcePosition: d => {
            const s = nodeMap.get(d.source_id);
            return s ? [s.lon, s.lat] : [0,0];
        },
        getTargetPosition: d => {
            const t = nodeMap.get(d.target_id);
            return t ? [t.lon, t.lat] : [0,0];
        },
        getSourceColor: d => d.color,
        getTargetColor: d => d.color,
        getWidth: 3,
        // Animate the arc by using the fraction of time elapsed
        // In a real Deck.gl app we might use a custom shader, but here we can 
        // approximate by adjusting the target position if we wanted a "packet" feel.
        // For now, we'll show the full arc while active.
    })
  ];

  deckgl.setProps({ layers });
}

function animate(timestamp) {
  if (!lastTimestamp) lastTimestamp = timestamp;
  const delta = timestamp - lastTimestamp;
  lastTimestamp = timestamp;

  if (!isPaused) {
    currentTime += delta;
    if (currentTime > 12000) currentTime = 0;
    
    slider.value = currentTime;
    timeDisplay.innerText = `${Math.floor(currentTime)} ms`;
    render();
  }

  requestAnimationFrame(animate);
}

slider.oninput = (e) => {
  currentTime = parseInt(e.target.value);
  timeDisplay.innerText = `${currentTime} ms`;
  render();
};

playBtn.onclick = () => {
  isPaused = !isPaused;
  playBtn.innerText = isPaused ? 'Play' : 'Pause';
};

loadData();
requestAnimationFrame(animate);
