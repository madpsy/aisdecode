const form          = document.getElementById('receiver-form');
const tbody         = document.getElementById('receivers-tbody');
const formTitle     = document.getElementById('form-title');
const cancelBtn     = document.getElementById('cancel-edit');
const searchInput   = document.getElementById('search-input');
const headers       = document.querySelectorAll('th.sortable');

// re-apply sorting + filtering on every keystroke
searchInput.addEventListener('input', applySortAndFilter);

let receiversData = [];
let editId        = null;
let sortKey       = 'id';
let sortDir       = 1; // 1 = ascending, -1 = descending

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
    
    // For date-based sorting (e.g., 'lastupdated')
    if (sortKey === 'lastupdated') {
      return (new Date(av) - new Date(bv)) * sortDir;
    }
    
    // For string-based sorting (e.g., 'name', 'description', 'ip_address')
    av = av.toString().toLowerCase();
    bv = bv.toString().toLowerCase();
    return av.localeCompare(bv) * sortDir;
  });

  // Filter
  const term = searchInput.value.trim().toLowerCase();
  const list = term
    ? receiversData.filter(r =>
        r.description.toLowerCase().includes(term) ||
        r.name.toLowerCase().includes(term)
      )
    : receiversData;

  renderList(list);
  updateHeaderIndicators();
}

function renderList(list) {
  tbody.innerHTML = '';
  list.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${r.id}</td>
      <td>${new Date(r.lastupdated).toLocaleString()}</td>
      <td>${r.name}</td>
      <td>${r.description}</td>
      <td>${r.latitude}</td>
      <td>${r.longitude}</td>
      <td>
        ${r.url
          ? `<a href="${r.url}" target="_blank">link</a>`
          : `—`
        }
      </td>
      <td>${r.ip_address}</td>
      <td>${r.messages ? r.messages : 'No messages'}</td> <!-- Add the messages here -->
      <td>
        <a class="action" data-id="${r.id}">Edit</a>
        &nbsp;|&nbsp;
        <button class="delete-btn" data-id="${r.id}">Delete</button>
      </td>
    `;
    tbody.appendChild(tr);
  });

  document.querySelectorAll('.action')
    .forEach(a => a.addEventListener('click', e => startEdit(e.target.dataset.id)));
  document.querySelectorAll('.delete-btn')
    .forEach(b => b.addEventListener('click', e => {
      const id = e.target.dataset.id;
      if (confirm(`Are you sure you want to delete receiver #${id}?`)) {
        fetch(`/admin/receivers/${id}`, { method: 'DELETE' })
          .then(res => {
            if (!res.ok) throw new Error(res.statusText);
            loadReceivers();
          })
          .catch(err => alert('Delete failed: ' + err));
      }
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
      document.getElementById('field-description').value = r.description;
      document.getElementById('field-latitude').value    = r.latitude;
      document.getElementById('field-longitude').value   = r.longitude;
      document.getElementById('field-url').value         = r.url ?? '';
    });
}

cancelBtn.onclick = () => {
  editId = null;
  formTitle.textContent = 'Add New Receiver';
  form.reset();
};

form.onsubmit = async e => {
  e.preventDefault();
  const payload = {
    name:        document.getElementById('field-name').value,
    description: document.getElementById('field-description').value,
    latitude:    parseFloat(document.getElementById('field-latitude').value),
    longitude:   parseFloat(document.getElementById('field-longitude').value),
  };
  const urlVal = document.getElementById('field-url').value.trim();
  if (urlVal) payload.url = urlVal;
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
    alert('Error: ' + await res.text());
    return;
  }
  form.reset();
  editId = null;
  formTitle.textContent = 'Add New Receiver';
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

window.addEventListener('load', loadReceivers);
