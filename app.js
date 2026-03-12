const { DeckGL, ScatterplotLayer, ArcLayer, LinearInterpolator } = deck;

let nodes = [];
let traffic = [];
let currentTime = 0;
let isPaused = false;
let playbackSpeed = 1.0;
let lastTimestamp = 0;
let hoveredNodeId = null;
let visibleRoles = new Set(['Builder', 'Proposer', 'PTC', 'Sampler', 'Provider']);
let visibleTopics = new Set(['/cl/ptc_attestation', '/cl/payload_envelope', '/cl/blob_sidecar', '/cl/beacon_block', '/cl/bids']);

const slider = document.getElementById('slider');
const playBtn = document.getElementById('play-btn');
const speedSelect = document.getElementById('speed-select');
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
  
  if (traffic.length > 0) {
      const minTime = Math.min(...traffic.map(t => t.start_time_ms));
      traffic.forEach(t => {
          t.start_time_ms -= minTime;
          t.arrival_time_ms -= minTime;
          t.start_time_ms %= 12000;
          t.arrival_time_ms %= 12000;
          if (t.arrival_time_ms < t.start_time_ms) t.arrival_time_ms += 12000;
      });
  }
  
  setupFilters();
  render();
}

function setupFilters() {
    // Node role filters
    document.querySelectorAll('.legend-item[data-role]').forEach(item => {
        const role = item.getAttribute('data-role');
        item.style.cursor = 'pointer';
        
        item.onclick = () => {
            if (visibleRoles.has(role)) {
                visibleRoles.delete(role);
                item.style.opacity = '0.3';
            } else {
                visibleRoles.add(role);
                item.style.opacity = '1.0';
            }
            render();
        };
    });

    // Traffic topic filters
    document.querySelectorAll('.legend-item[data-topic]').forEach(item => {
        const topic = item.getAttribute('data-topic');
        item.style.cursor = 'pointer';
        
        item.onclick = () => {
            if (visibleTopics.has(topic)) {
                visibleTopics.delete(topic);
                item.style.opacity = '0.3';
            } else {
                visibleTopics.add(topic);
                item.style.opacity = '1.0';
            }
            render();
        };
    });
}

function render() {
  const filteredNodes = nodes.filter(n => visibleRoles.has(n.role));
  const nodeMap = new Map(filteredNodes.map(n => [n.id, n]));

  // Filter traffic by time, visible roles, AND visible topics
  const activeTraffic = traffic.filter(t => {
      const isVisibleTime = currentTime >= t.start_time_ms && currentTime <= t.arrival_time_ms;
      const isVisibleNodes = nodeMap.has(t.source_id) && nodeMap.has(t.target_id);
      const baseTopic = '/' + t.topic.split('/').slice(1, 3).join('/');
      const isVisibleTopic = visibleTopics.has(baseTopic);
      
      return isVisibleTime && isVisibleNodes && isVisibleTopic;
  });

  const packets = activeTraffic.map(t => {
      const s = nodeMap.get(t.source_id);
      const target = nodeMap.get(t.target_id);
      if (!s || !target) return null;

      const duration = t.arrival_time_ms - t.start_time_ms;
      const elapsed = currentTime - t.start_time_ms;
      const ratio = Math.min(1, Math.max(0, elapsed / duration));

      const isHighlighted = hoveredNodeId === t.source_id;

      return {
          ...t,
          position: [
              s.lon + (target.lon - s.lon) * ratio,
              s.lat + (target.lat - s.lat) * ratio
          ],
          radius: (t.topic.includes('ptc_attestation') ? 15 : 8) * (isHighlighted ? 1.5 : 1),
          opacity: hoveredNodeId ? (isHighlighted ? 255 : 50) : 255
      };
  }).filter(p => p !== null);

  const layers = [
    new ScatterplotLayer({
      id: 'nodes',
      data: filteredNodes,
      getPosition: d => [d.lon, d.lat],
      getFillColor: d => {
          if (d.id === hoveredNodeId) return [255, 255, 255];
          const isActiveSource = activeTraffic.some(t => t.source_id === d.id && (currentTime - t.start_time_ms) < 200);
          if (isActiveSource) return [255, 255, 255];
          return d.color;
      },
      getRadius: d => (d.role === 'PTC' ? 15 : 10) * (d.id === hoveredNodeId ? 1.3 : 1),
      radiusMinPixels: 5,
      pickable: true,
      onHover: info => {
          hoveredNodeId = info.object ? info.object.id : null;
          render();
      },
      updateTriggers: {
          getFillColor: [currentTime, hoveredNodeId],
          getRadius: [hoveredNodeId]
      }
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
        getSourceColor: d => {
            const alpha = hoveredNodeId ? (hoveredNodeId === d.source_id ? 200 : 20) : 50;
            return [...d.color, alpha];
        },
        getTargetColor: d => {
            const alpha = hoveredNodeId ? (hoveredNodeId === d.source_id ? 200 : 20) : 50;
            return [...d.color, alpha];
        },
        getWidth: d => (hoveredNodeId === d.source_id ? 4 : 1),
        updateTriggers: {
            getSourceColor: [hoveredNodeId],
            getTargetColor: [hoveredNodeId],
            getWidth: [hoveredNodeId]
        }
    }),
    new ScatterplotLayer({
        id: 'packets',
        data: packets,
        getPosition: d => d.position,
        getFillColor: d => [...d.color, d.opacity],
        getRadius: d => d.radius,
        radiusMinPixels: 3,
        updateTriggers: {
            getPosition: [currentTime],
            getFillColor: [hoveredNodeId],
            getRadius: [hoveredNodeId]
        }
    })
  ];

  deckgl.setProps({ layers });
}

function animate(timestamp) {
  if (!lastTimestamp) lastTimestamp = timestamp;
  const delta = timestamp - lastTimestamp;
  lastTimestamp = timestamp;

  if (!isPaused) {
    currentTime += delta * playbackSpeed;
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
  render();
};

speedSelect.onchange = (e) => {
    playbackSpeed = parseFloat(e.target.value);
};

loadData();
requestAnimationFrame(animate);
