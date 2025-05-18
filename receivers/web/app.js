const form          = document.getElementById('receiver-form');
const tbody         = document.getElementById('receivers-tbody');
const formTitle     = document.getElementById('form-title');
const cancelBtn     = document.getElementById('cancel-edit');
const searchInput   = document.getElementById('search-input');
const notSeenFilter = document.getElementById('not-seen-filter');
const headers       = document.querySelectorAll('th.sortable');

// re-apply sorting + filtering on every keystroke
searchInput.addEventListener('input', applySortAndFilter);
// re-apply sorting + filtering when checkbox is toggled
notSeenFilter.addEventListener('change', applySortAndFilter);

let receiversData = [];
let editId        = null;
let sortKey       = 'lastseen';
let sortDir       = -1; // 1 = ascending, -1 = descending
let map           = null;
let marker        = null;
let overlayDiv    = null;

// Function to display anonymous port message if applicable
function updateAnonMessage() {
  const hasAnonData = receiversData.some(r => r.id === 0 && r.message_stats && Object.keys(r.message_stats).length > 0);
  const formSection = document.getElementById('form-section');
  const existingMsg = document.getElementById('anon-msg');
  if (existingMsg) {
    existingMsg.remove();
  }
  if (hasAnonData) {
    const msgEl = document.createElement('div');
    msgEl.id = 'anon-msg';
    msgEl.textContent = 'Anonymous port has seen data recently';
    const titleEl = document.getElementById('form-title');
    titleEl.insertAdjacentElement('beforebegin', msgEl);
  }
}
async function loadReceivers() {
  const res = await fetch('/admin/receivers');
  receiversData = await res.json();
  applySortAndFilter();
}

function applySortAndFilter() {
  // Sort
  receiversData.sort((a, b) => {
    let av = a[sortKey], bv = b[sortKey];
    
    // Check if sorting by numeric fields like 'id', 'latitude', 'longitude', or 'messages'
    if (sortKey === 'id' || sortKey === 'latitude' || sortKey === 'longitude' || sortKey === 'messages') {
      return (av - bv) * sortDir;
    }
    
    // For date-based sorting (e.g., 'lastupdated', 'lastseen')
    if (sortKey === 'lastupdated' || sortKey === 'lastseen') {
      if (sortKey === 'lastseen') {
        if (!av) return 1 * sortDir;  // Move null/undefined values to the end
        if (!bv) return -1 * sortDir; // Move null/undefined values to the end
      }
      const diff = (new Date(av) - new Date(bv)) * sortDir;
      if (diff !== 0) return diff;
      return a.id - b.id;
    }
    
    // For string-based sorting (e.g., 'name', 'description', 'ip_address')
    av = av.toString().toLowerCase();
    bv = bv.toString().toLowerCase();
    return av.localeCompare(bv) * sortDir;
  });

  // Filter
  const term = searchInput.value.trim().toLowerCase();
  const notSeenFilterChecked = notSeenFilter.checked;
  
  // First filter by search term
  let filteredList = term
    ? receiversData.filter(r => {
        // Check description and name
        if (r.description.toLowerCase().includes(term) ||
            r.name.toLowerCase().includes(term)) {
          return true;
        }
        
        // Check IP address - both in ip_address field and message_stats
        if (r.ip_address && r.ip_address.toLowerCase().includes(term)) {
          return true;
        }
        
        // Check in message_stats for IP addresses
        if (r.message_stats && Object.keys(r.message_stats).some(ip =>
          ip.toLowerCase().includes(term))) {
          return true;
        }
        
        // Check UDP port
        if (r.udp_port !== undefined && r.udp_port.toString().includes(term)) {
          return true;
        }
        
        return false;
      })
    : receiversData;
    
  // Then apply the "Not seen >1 week" filter if checked
  if (notSeenFilterChecked) {
    const oneWeekAgo = new Date();
    oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);
    
    filteredList = filteredList.filter(r => {
      // If lastseen is null/undefined, it's never been seen, so include it
      if (!r.lastseen) return true;
      
      // Compare the last seen date with 1 week ago
      const lastSeenDate = new Date(r.lastseen);
      return lastSeenDate < oneWeekAgo;
    });
  }
  
  const list = filteredList;

  renderList(list);
  updateHeaderIndicators();
  updateAnonMessage();
}

function renderList(list) {
  tbody.innerHTML = '';
  
  // Get all column headers and their data-key attributes
  const headers = document.querySelectorAll('th.sortable');
  const columnKeys = Array.from(headers).map(th => th.getAttribute('data-key'));
  
  list.forEach(r => {
    const tr = document.createElement('tr');
    
    // Create cells with appropriate classes based on column keys
    const cells = [
      `<td class="col-id">${r.id}</td>`,
      `<td class="col-lastseen">${r.lastseen ? new Date(r.lastseen).toLocaleString() : '—'}</td>`,
      `<td class="col-lastupdated">${new Date(r.lastupdated).toLocaleString()}</td>`,
      `<td class="col-name">${r.name}</td>`,
      `<td class="col-description">${r.description}</td>`,
      `<td class="col-latitude">${r.latitude}</td>`,
      `<td class="col-longitude">${r.longitude}</td>`,
      `<td class="col-url">${r.url ? `<a href="${r.url}" target="_blank">link</a>` : `—`}</td>`,
      `<td class="col-ip_address">${r.message_stats ?
        Object.keys(r.message_stats).map(ip =>
          `${ip} (${r.message_stats[ip].message_count})`
        ).join('<br>') :
        r.ip_address || '—'}</td>`,
      `<td class="col-udp_port">${r.udp_port !== undefined ? r.udp_port : '—'}</td>`,
      `<td class="col-password">••••••••</td>`,
      `<td class="col-messages">${r.messages !== undefined && r.messages !== null ? r.messages : 'No messages'}</td>`,
      `<td class="col-actions">${r.id === 0 ?
        '—' :
        `<a class="action" data-id="${r.id}">Edit</a>
         &nbsp;|&nbsp;
         <button class="delete-btn" data-id="${r.id}" data-name="${r.name}">Delete</button>`}</td>`
    ];
    
    tr.innerHTML = cells.join('');
    tbody.appendChild(tr);
  });

  document.querySelectorAll('.action')
    .forEach(a => a.addEventListener('click', e => startEdit(e.target.dataset.id)));
  document.querySelectorAll('.delete-btn')
    .forEach(b => b.addEventListener('click', e => {
      const id = e.target.dataset.id;
      const name = e.target.dataset.name;
      showDeleteModal(id, name);
    }));
}

function startEdit(id) {
  fetch(`/admin/receivers/${id}`)
    .then(r => r.json())
    .then(r => {
      editId = r.id;
      formTitle.textContent = 'Edit Receiver #' + r.id;
      document.getElementById('field-id').value          = r.id;
      document.getElementById('field-name').value        = r.name;
      document.getElementById('field-email').value       = r.email;
      document.getElementById('field-notifications').checked = r.notifications !== undefined ? r.notifications : true;
      document.getElementById('field-description').value = r.description;
      document.getElementById('field-latitude').value    = r.latitude;
      document.getElementById('field-longitude').value   = r.longitude;
      document.getElementById('field-url').value         = r.url ?? '';
      // IP address field removed - automatically tracked by collector system
      document.getElementById('field-udp-port').value    = r.udp_port !== undefined ? r.udp_port : '';
      document.getElementById('field-password').value    = r.password !== undefined ? r.password : '';
      
      // Show regenerate button when editing
      document.getElementById('regenerate-password').style.display = 'inline-block';
      
      // Update the map with the receiver's location
      if (map) {
        const latlng = L.latLng(r.latitude, r.longitude);
        updateMarkerPosition(latlng);
        map.setView(latlng, 10);
        updateOverlay(latlng);
      } else {
        // Initialize the map if it doesn't exist
        setTimeout(() => {
          initMap();
        }, 100);
      }
      
      // Scroll to the top of the page to show the form
      window.scrollTo({
        top: 0,
        behavior: 'smooth' // Add smooth scrolling for better user experience
      });
    });
}

cancelBtn.onclick = () => {
  editId = null;
  formTitle.textContent = 'Add New Receiver';
  form.reset();
  // IP address field removed - automatically tracked by collector system
  document.getElementById('field-udp-port').value = '';
  document.getElementById('field-password').value = '';
  // Hide regenerate button for new receivers (will be auto-generated)
  document.getElementById('regenerate-password').style.display = 'none';
  
  // Reset the map to UK view and remove marker
  if (map) {
    // Remove marker if it exists
    if (marker) {
      map.removeLayer(marker);
      marker = null;
    }
    
    // Center on UK with zoom level 5
    map.setView([54.5, -3.5], 5);
    
    // Hide the overlay
    if (overlayDiv) {
      overlayDiv.style.display = 'none';
    }
  }
};

// Handle password regeneration
document.getElementById('regenerate-password').addEventListener('click', function(e) {
  e.preventDefault();
  
  if (!editId) {
    // For new receivers, just clear the field - a new password will be generated on save
    document.getElementById('field-password').value = '';
    return;
  }
  
  // For existing receivers, call the API to regenerate the password
  fetch(`/admin/receivers/regenerate-password/${editId}`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'}
  })
  .then(response => {
    if (!response.ok) {
      throw new Error('Failed to regenerate password');
    }
    return response.json();
  })
  .then(data => {
    document.getElementById('field-password').value = data.password;
  })
  .catch(error => {
    alert('Error: ' + error.message);
  });
});

form.onsubmit = async e => {
  e.preventDefault();
  const payload = {
    name:         document.getElementById('field-name').value,
    email:        document.getElementById('field-email').value,
    notifications: document.getElementById('field-notifications').checked,
    description:  document.getElementById('field-description').value,
    latitude:     parseFloat(document.getElementById('field-latitude').value),
    longitude:    parseFloat(document.getElementById('field-longitude').value),
  };
  const urlVal = document.getElementById('field-url').value.trim();
  if (urlVal) payload.url = urlVal;
  
  // IP address field removed - automatically tracked by collector system
  
  // Include password in payload if it exists
  const passwordVal = document.getElementById('field-password').value.trim();
  if (passwordVal) payload.password = passwordVal;
  const method = editId ? 'PUT'  : 'POST';
  const url    = editId
                ? `/admin/receivers/${editId}`
                : '/admin/receivers';
  const res    = await fetch(url, {
    method,
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  });
  
  if (!res.ok) {
    const errorText = await res.text();
    // Check if the error is related to duplicate name
    if (errorText.includes("already in use")) {
      // Highlight the name field to indicate the error
      const nameField = document.getElementById('field-name');
      nameField.classList.add('error');
      nameField.focus();
      
      // Show a more user-friendly error message
      alert('Error: ' + errorText + '\nPlease choose a different name.');
      
      // Add an event listener to remove the error class when the user starts typing
      nameField.addEventListener('input', function onInput() {
        nameField.classList.remove('error');
        nameField.removeEventListener('input', onInput);
      }, { once: true });
    } else {
      // For other errors, show the generic error message
      alert('Error: ' + errorText);
    }
    return;
  }
  
  form.reset();
  editId = null;
  formTitle.textContent = 'Add New Receiver';
  // IP address field removed - automatically tracked by collector system
  document.getElementById('field-udp-port').value = '';
  document.getElementById('field-password').value = '';
  document.getElementById('regenerate-password').style.display = 'none';
  loadReceivers();
};

headers.forEach(th => th.addEventListener('click', () => {
  const key = th.dataset.key;
  if (sortKey === key) sortDir = -sortDir;
  else {
    sortKey = key;
    sortDir = 1;
  }
  applySortAndFilter();
}));

function updateHeaderIndicators() {
  headers.forEach(th => {
    const key = th.dataset.key;
    th.textContent = th.textContent.replace(/ *[▲▼]$/, '');
    if (key === sortKey) {
      th.textContent += sortDir === 1 ? ' ▲' : ' ▼';
    }
  });
}

// Delete confirmation modal functionality
const deleteModal = document.getElementById('delete-modal');
const deleteReceiverId = document.getElementById('delete-receiver-id');
const deleteReceiverName = document.getElementById('delete-receiver-name');
const deleteConfirmInput = document.getElementById('delete-confirm-input');
const deleteConfirmBtn = document.getElementById('delete-confirm-btn');
const deleteCancelBtn = document.getElementById('delete-cancel-btn');

// Show the delete confirmation modal
function showDeleteModal(id, name) {
  deleteReceiverId.value = id;
  deleteReceiverName.textContent = name;
  deleteConfirmInput.value = '';
  deleteConfirmBtn.disabled = true;
  deleteConfirmInput.classList.remove('valid', 'invalid');
  deleteModal.classList.add('show');
  deleteConfirmInput.focus();
}

// Hide the delete confirmation modal
function hideDeleteModal() {
  deleteModal.classList.remove('show');
}

// Validate the delete confirmation input
deleteConfirmInput.addEventListener('input', () => {
  const value = deleteConfirmInput.value.trim();
  
  if (value === 'delete') {
    deleteConfirmInput.classList.add('valid');
    deleteConfirmInput.classList.remove('invalid');
    deleteConfirmBtn.disabled = false;
  } else if (value.length > 0) {
    deleteConfirmInput.classList.add('invalid');
    deleteConfirmInput.classList.remove('valid');
    deleteConfirmBtn.disabled = true;
  } else {
    deleteConfirmInput.classList.remove('valid', 'invalid');
    deleteConfirmBtn.disabled = true;
  }
});

// Handle delete confirmation
deleteConfirmBtn.addEventListener('click', () => {
  const id = deleteReceiverId.value;
  
  fetch(`/admin/receivers/${id}`, { method: 'DELETE' })
    .then(res => {
      if (!res.ok) throw new Error(res.statusText);
      hideDeleteModal();
      loadReceivers();
    })
    .catch(err => {
      hideDeleteModal();
      alert('Delete failed: ' + err);
    });
});

// Handle delete cancellation
deleteCancelBtn.addEventListener('click', hideDeleteModal);

// Close modal when clicking outside of it
deleteModal.addEventListener('click', (e) => {
  if (e.target === deleteModal) {
    hideDeleteModal();
  }
});

// Close modal with Escape key
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && deleteModal.classList.contains('show')) {
    hideDeleteModal();
  }
});

// Initialize the map
function initMap() {
  // Create the map if it doesn't exist
  if (!map) {
    // Default to UK view if no coordinates are set
    map = L.map('location-map').setView([54.5, -3.5], 5); // UK centered view
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);

    // Create overlay for coordinates
    overlayDiv = document.createElement('div');
    overlayDiv.className = 'latlng-overlay';
    map.getContainer().appendChild(overlayDiv);
    
    // Add click handler to the map
    map.on('click', function(e) {
      updateMarkerPosition(e.latlng);
      updateLatLngFields(e.latlng);
      updateOverlay(e.latlng);
    });
    
    // Handle map resize when form becomes visible
    setTimeout(() => map.invalidateSize(), 0);
    window.addEventListener('resize', () => map.invalidateSize());
  }
  
  // Reset the marker if it exists
  if (marker) {
    map.removeLayer(marker);
    marker = null;
  }
  
  // Get initial coordinates from the form fields
  const latField = document.getElementById('field-latitude');
  const lngField = document.getElementById('field-longitude');
  const lat = parseFloat(latField.value);
  const lng = parseFloat(lngField.value);
  
  // Check if we have valid coordinates
  const hasValidCoordinates = !isNaN(lat) && !isNaN(lng) &&
                             (lat !== 0 || lng !== 0) &&
                             latField.value.trim() !== '' &&
                             lngField.value.trim() !== '';
  
  if (hasValidCoordinates) {
    // Create a new marker only if we have valid coordinates
    const latlng = L.latLng(lat, lng);
    marker = L.marker(latlng, { draggable: true }).addTo(map);
    
    // Update fields when marker is dragged
    marker.on('dragend', function() {
      const position = marker.getLatLng();
      updateLatLngFields(position);
      updateOverlay(position);
    });
    
    // Update overlay when marker is dragged
    marker.on('drag', function() {
      updateOverlay(marker.getLatLng());
    });
    
    // Center the map on the marker
    map.setView(latlng, 10);
    
    // Update the overlay
    updateOverlay(latlng);
  } else {
    // If no valid coordinates, hide the overlay
    if (overlayDiv) {
      overlayDiv.style.display = 'none';
    }
    
    // Center on UK with zoom level 5
    map.setView([54.5, -3.5], 5);
  }
}

// Update the lat/lng fields when the marker is moved
function updateLatLngFields(latlng) {
  document.getElementById('field-latitude').value = latlng.lat.toFixed(6);
  document.getElementById('field-longitude').value = latlng.lng.toFixed(6);
}

// Update the marker position when the lat/lng fields are changed
function updateMarkerPosition(latlng) {
  if (marker) {
    marker.setLatLng(latlng);
  } else {
    // Create a new marker if one doesn't exist
    marker = L.marker(latlng, { draggable: true }).addTo(map);
    
    // Update fields when marker is dragged
    marker.on('dragend', function() {
      const position = marker.getLatLng();
      updateLatLngFields(position);
      updateOverlay(position);
    });
    
    // Update overlay when marker is dragged
    marker.on('drag', function() {
      updateOverlay(marker.getLatLng());
    });
  }
}

// Update the overlay with the current coordinates
function updateOverlay(latlng) {
  if (overlayDiv && latlng) {
    overlayDiv.textContent = `${latlng.lat.toFixed(6)}, ${latlng.lng.toFixed(6)}`;
    overlayDiv.style.display = 'block';
  }
}

// Function to handle coordinate field updates
function handleCoordinateChange() {
  const latField = document.getElementById('field-latitude');
  const lngField = document.getElementById('field-longitude');
  const lat = parseFloat(latField.value);
  const lng = parseFloat(lngField.value);
  
  // Check if we have valid coordinates
  const hasValidCoordinates = !isNaN(lat) && !isNaN(lng) &&
                             (lat !== 0 || lng !== 0) &&
                             latField.value.trim() !== '' &&
                             lngField.value.trim() !== '';
  
  if (hasValidCoordinates) {
    const latlng = L.latLng(lat, lng);
    updateMarkerPosition(latlng);
    map.setView(latlng, 10); // Use zoom level 10 for better visibility
    updateOverlay(latlng);
  } else if (marker) {
    // If coordinates are invalid but we have a marker, remove it
    map.removeLayer(marker);
    marker = null;
    
    // Hide the overlay
    if (overlayDiv) {
      overlayDiv.style.display = 'none';
    }
    
    // Center on UK with zoom level 5
    map.setView([54.5, -3.5], 5);
  }
}

// Add event listeners to the lat/lng fields
const latField = document.getElementById('field-latitude');
const lngField = document.getElementById('field-longitude');

// Use both change and blur events for better reliability
latField.addEventListener('change', handleCoordinateChange);
latField.addEventListener('blur', handleCoordinateChange);
latField.addEventListener('keyup', function(e) {
  if (e.key === 'Enter') {
    handleCoordinateChange();
  }
});

lngField.addEventListener('change', handleCoordinateChange);
lngField.addEventListener('blur', handleCoordinateChange);
lngField.addEventListener('keyup', function(e) {
  if (e.key === 'Enter') {
    handleCoordinateChange();
  }
});

window.addEventListener('load', () => {
  loadReceivers();
  // Hide regenerate button initially (for new receivers)
  document.getElementById('regenerate-password').style.display = 'none';
  // Initialize the map
  initMap();
});
