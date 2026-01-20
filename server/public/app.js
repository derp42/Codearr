import { TREE_ELEMENT_DEFS, loadPluginDefs } from "./tree-elements/elements.js";
import { normalizeElementNode } from "./tree-elements/base.js";

const view = document.getElementById("view");
const title = document.getElementById("view-title");
const statusEl = document.getElementById("server-status");
const navItems = document.querySelectorAll(".nav-item");
const subnavContainers = new Map(
  Array.from(document.querySelectorAll(".subnav")).map((el) => [el.dataset.subnav, el])
);
let showStaleNodes = false;
let activeView = "dashboard";
let refreshToken = 0;
const expandedNodes = new Set();
const expandedLibraries = new Set();
const libraryFilesPage = new Map();
let isLibraryModalOpen = false;
let isLogModalOpen = false;
let isTreeModalOpen = false;
let isTreeAssignModalOpen = false;
let isTreeDesignerActive = false;
let treeDesignerRoot = null;
let treeDesignerState = { treeId: null };
let pendingTreeId = null;
let currentLogJobId = null;
let currentLogEntries = [];
const collapsedJobSections = new Set();
const jobSectionPage = new Map();
const jobSectionPageSize = new Map();
const expandedNav = new Set();
const libraryFormState = {
  name: "",
  path: "",
  includeExtensions: "",
  excludeExtensions: "",
  nodes: "",
  scanIntervalMin: "",
};

fetchJson("/api/trees/plugins")
  .then((files) => loadPluginDefs(Array.isArray(files) ? files : []))
  .then((plugins) => {
    if (Array.isArray(plugins) && plugins.length) {
      TREE_ELEMENT_DEFS.push(...plugins);
    }
  })
  .catch(() => {});

const routes = {
  dashboard: renderDashboard,
  libraries: renderLibraries,
  nodes: renderNodes,
  jobs: renderJobs,
  trees: renderTrees,
};


navItems.forEach((item) => {
  item.addEventListener("click", (event) => {
    const viewName = item.dataset.view;
    const toggle = event.target.closest("[data-nav-toggle]");
    if (toggle) {
      toggleNav(viewName);
      return;
    }
    setActiveView(viewName, item);
  });
});

document.querySelectorAll(".subnav").forEach((container) => {
  container.addEventListener("click", (event) => {
    const target = event.target.closest("button[data-subnav-view]");
    if (!target) return;
    const viewName = target.dataset.subnavView;
    if (viewName === "trees") {
      pendingTreeId = target.dataset.treeId ?? null;
    }
    setActiveView(viewName);
  });
});

function setActiveView(viewName, clickedItem) {
  navItems.forEach((el) => el.classList.remove("active"));
  if (clickedItem) clickedItem.classList.add("active");
  else {
    document.querySelectorAll(`.nav-item[data-view='${viewName}']`).forEach((el) => {
      el.classList.add("active");
    });
  }
  title.textContent = viewName[0].toUpperCase() + viewName.slice(1);
  activeView = viewName;
  if (viewName !== "trees") {
    isTreeDesignerActive = false;
    treeDesignerState.treeId = null;
  }
  routes[viewName]();
  startAutoRefresh();
}

function toggleNav(viewName) {
  if (expandedNav.has(viewName)) {
    expandedNav.delete(viewName);
  } else {
    expandedNav.add(viewName);
  }
  renderSubnavVisibility();
}

function renderSubnavVisibility() {
  subnavContainers.forEach((container, key) => {
    container.classList.toggle("expanded", expandedNav.has(key));
  });
  document.querySelectorAll(".nav-item[data-expandable='true']").forEach((item) => {
    const viewName = item.dataset.view;
    item.classList.toggle("expanded", expandedNav.has(viewName));
  });
}

async function checkHealth() {
  try {
    const res = await fetch("/api/health");
    if (!res.ok) throw new Error("Server offline");
    statusEl.textContent = "Server online";
  } catch {
    statusEl.textContent = "Server offline";
  }
}

async function renderDashboard() {
  const [libs, nodes, stats] = await Promise.all([
    fetchJson("/api/libraries"),
    fetchJson("/api/nodes"),
    fetchJson("/api/jobs/stats"),
  ]);
  const counts = buildJobCounts(stats);

  view.innerHTML = `
    <div class="card-grid">
      <div class="card">
        <h3>Libraries</h3>
        <p class="muted">${libs.length} configured</p>
      </div>
      <div class="card">
        <h3>Nodes</h3>
        <p class="muted">${nodes.length} connected</p>
      </div>
      <div class="card">
        <h3>Healthcheck</h3>
        <p class="muted">${counts.healthcheckActive}</p>
      </div>
      <div class="card">
        <h3>Health Failed</h3>
        <p class="muted">${counts.healthcheckError}</p>
      </div>
      <div class="card">
        <h3>Transcode</h3>
        <p class="muted">${counts.transcodeActive}</p>
      </div>
      <div class="card">
        <h3>Transcode Successful</h3>
        <p class="muted">${counts.transcodeSuccess}</p>
      </div>
      <div class="card">
        <h3>Transcode Failed</h3>
        <p class="muted">${counts.transcodeError}</p>
      </div>
    </div>
  `;
}

async function renderLibraries() {
  const existingName = document.querySelector("#library-form input[name='name']")?.value;
  const existingPath = document.querySelector("#library-form input[name='path']")?.value;
  const existingInclude = document.querySelector("#library-form input[name='includeExtensions']")?.value;
  const existingExclude = document.querySelector("#library-form input[name='excludeExtensions']")?.value;
  const existingNodes = document.querySelector("#library-form input[name='nodes']")?.value;
  const existingScan = document.querySelector("#library-form input[name='scanIntervalMin']")?.value;
  if (existingName !== undefined) libraryFormState.name = existingName;
  if (existingPath !== undefined) libraryFormState.path = existingPath;
  if (existingInclude !== undefined) libraryFormState.includeExtensions = existingInclude;
  if (existingExclude !== undefined) libraryFormState.excludeExtensions = existingExclude;
  if (existingNodes !== undefined) libraryFormState.nodes = existingNodes;
  if (existingScan !== undefined) libraryFormState.scanIntervalMin = existingScan;

  const libs = await fetchJson("/api/libraries");
  const nodes = await fetchJson("/api/nodes");
  const nodeNames = nodes.map((node) => node.name).sort();

  view.innerHTML = `
    <div class="card">
      <div class="card-header">
        <div>
          <h3>Libraries</h3>
          <p class="muted">Manage library entries and scan settings.</p>
        </div>
        <button class="primary" id="add-library">Add Library</button>
      </div>
    </div>
    <div class="card" style="margin-top:16px;">
      <h3>Configured Libraries</h3>
      <table class="table">
        <thead>
          <tr><th>Name</th><th>Path</th><th>Files</th><th>Added</th><th></th></tr>
        </thead>
        <tbody>
          ${libs
            .map(
              (lib) => {
                const isExpanded = expandedLibraries.has(lib.id);
                const detailsRow = renderLibraryDetailsRow(lib, isExpanded);
                return `
                  <tr>
                    <td>${lib.name}</td>
                    <td>${lib.path}</td>
                    <td>${lib.file_count ?? 0}</td>
                    <td>${new Date(lib.created_at).toLocaleString()}</td>
                    <td>
                      <button class="ghost" data-library-toggle="${lib.id}">Files</button>
                      <button class="ghost" data-library-edit="${lib.id}">Edit</button>
                    </td>
                  </tr>
                  ${detailsRow}
                `;
              }
            )
            .join("")}
        </tbody>
      </table>
    </div>
    ${renderLibraryModal(nodeNames)}
  `;

  const addBtn = document.getElementById("add-library");
  addBtn.addEventListener("click", () => openLibraryModal());

  const toggleButtons = view.querySelectorAll("button[data-library-toggle]");
  toggleButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const libId = btn.dataset.libraryToggle;
      if (expandedLibraries.has(libId)) {
        expandedLibraries.delete(libId);
      } else {
        expandedLibraries.add(libId);
      }
      await renderLibraries();
    });
  });

  const editButtons = view.querySelectorAll("button[data-library-edit]");
  editButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const libId = btn.dataset.libraryEdit;
      const lib = libs.find((item) => item.id === libId);
      openLibraryModal(lib);
    });
  });

  bindLibraryModal(libs);
}

async function renderNodes() {
  const nodes = await fetchJson(`/api/nodes${showStaleNodes ? "?include_stale=1" : ""}`);

  view.innerHTML = `
    <div class="card">
      <div class="card-header">
        <div>
          <h3>Connected Nodes</h3>
          <p class="muted">${showStaleNodes ? "Showing active + stale" : "Showing active only"}</p>
        </div>
        <label class="toggle">
          <input type="checkbox" id="toggle-stale" ${showStaleNodes ? "checked" : ""} />
          <span>Show stale</span>
        </label>
      </div>
      <table class="table">
        <thead>
          <tr><th>Name</th><th>Status</th><th>Platform</th><th>CPU</th><th>RAM</th><th>GPU</th><th></th></tr>
        </thead>
        <tbody>
          ${nodes
            .map((node) => {
              const cpuValue = node.metrics?.cpu?.load;
              const cpuText = typeof cpuValue === "number" ? `${cpuValue.toFixed(1)}%` : "-";
              const ramPercent = node.metrics?.memory?.usedPercent;
              const ramUsed = node.metrics?.memory?.usedBytes;
              const ramTotal = node.hardware?.memory?.totalBytes ?? node.metrics?.memory?.totalBytes;
              const ramDetailText = formatRamUsage(ramUsed, ramTotal);
              const ramText =
                typeof ramPercent === "number"
                  ? `${ramPercent.toFixed(1)}% (${ramDetailText})`
                  : "-";

              const gpus = mergeGpuLists(node.hardware?.gpus, node.metrics?.gpus);
              const gpuUtil = averageGpuUtil(gpus);
              const gpuSummary =
                typeof gpuUtil === "number" ? `${gpuUtil.toFixed(1)}% (${gpus.length})` : "-";
              const status = node.stale ? "stale" : "active";
              const staleClass = node.stale ? "stale" : "";
              const isExpanded = expandedNodes.has(node.id);
              const detailsRow = renderGpuDetailsRow(
                node.id,
                gpus,
                isExpanded,
                node.last_seen,
                ramDetailText
              );
              return `<tr>
                <td>${node.name}</td>
                <td><span class="status-pill ${staleClass}">${status}</span></td>
                <td>${node.platform}</td>
                <td class="usage-cell">${renderUsageMeter(cpuValue, cpuText)}</td>
                <td class="usage-cell">${renderUsageMeter(ramPercent, ramText, true)}</td>
                <td class="usage-cell">${renderUsageMeter(gpuUtil, gpuSummary)}</td>
                <td>
                  ${
                    node.stale
                      ? `<button class="danger" data-node-id="${node.id}">Delete</button>`
                      : `<button class="ghost" data-gpu-toggle="${node.id}">Details</button>`
                  }
                </td>
              </tr>${detailsRow}`;
            })
            .join("")}
        </tbody>
      </table>
    </div>
  `;

  const toggle = document.getElementById("toggle-stale");
  toggle.addEventListener("change", async () => {
    showStaleNodes = toggle.checked;
    await renderNodes();
  });

  const deleteButtons = view.querySelectorAll("button[data-node-id]");
  deleteButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const nodeId = btn.dataset.nodeId;
      await fetch(`/api/nodes/${encodeURIComponent(nodeId)}`, { method: "DELETE" });
      await renderNodes();
    });
  });

  const gpuButtons = view.querySelectorAll("button[data-gpu-toggle]");
  gpuButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const nodeId = btn.dataset.gpuToggle;
      if (expandedNodes.has(nodeId)) {
        expandedNodes.delete(nodeId);
      } else {
        expandedNodes.add(nodeId);
      }
      await renderNodes();
    });
  });
}

async function renderJobs() {
  const [jobs, config] = await Promise.all([
    fetchJson("/api/jobs/queue?limit=1000"),
    fetchJson("/api/config"),
  ]);

  const isActiveStatus = (status) => status === "queued" || status === "processing";
  const sections = [
    {
      title: "Healthcheck",
      key: "healthcheck",
      filter: (job) => job.type === "healthcheck" && isActiveStatus(normalizeJobStatus(job.status)),
    },
    {
      title: "Health Failed",
      key: "health_failed",
      filter: (job) => job.type === "healthcheck" && normalizeJobStatus(job.status) === "error",
    },
    {
      title: "Transcode",
      key: "transcode",
      filter: (job) => job.type === "transcode" && isActiveStatus(normalizeJobStatus(job.status)),
    },
    {
      title: "Transcode Successful",
      key: "transcode_successful",
      filter: (job) => job.type === "transcode" && normalizeJobStatus(job.status) === "successful",
    },
    {
      title: "Failed Transcode",
      key: "transcode_failed",
      filter: (job) => job.type === "transcode" && normalizeJobStatus(job.status) === "error",
    },
  ];

  view.innerHTML = `
    <div class="card">
      <div class="card-header">
        <div>
          <h3>Recent Jobs</h3>
          <p class="muted">Re-enqueue failed or unhealthy jobs.</p>
        </div>
        <div class="actions">
          <button class="ghost" id="reenqueue-health-failed">Re-enqueue health failed</button>
          <button class="ghost" id="reenqueue-transcode-failed">Re-enqueue failed transcodes</button>
          ${
            config.debug
              ? `<button class="danger" id="reset-jobs">Reset jobs</button>`
              : ""
          }
        </div>
      </div>
      ${sections
        .map((section) => {
          const rows = jobs.filter(section.filter);
          const isCollapsed = collapsedJobSections.has(section.key);
          const pageSize = jobSectionPageSize.get(section.key) ?? 25;
          const page = jobSectionPage.get(section.key) ?? 0;
          const totalPages = pageSize === Infinity ? 1 : Math.max(Math.ceil(rows.length / pageSize), 1);
          const start = pageSize === Infinity ? 0 : page * pageSize;
          const visibleRows = pageSize === Infinity ? rows : rows.slice(start, start + pageSize);
          return `
            <div class="card" style="margin-top:16px;">
              <div class="card-header">
                <h4>${section.title} <span class="muted">(${rows.length})</span></h4>
                <button class="ghost" data-job-section-toggle="${section.key}">
                  ${isCollapsed ? "Expand" : "Collapse"}
                </button>
              </div>
              <div class="jobs-section-controls ${isCollapsed ? "hidden" : ""}">
                <label class="muted">Show</label>
                <select data-job-page-size="${section.key}">
                  ${[10, 25, 50, 100, "all"]
                    .map((size) => {
                      const value = size === "all" ? "all" : String(size);
                      const selected =
                        (size === "all" && pageSize === Infinity) || pageSize === Number(size);
                      return `<option value="${value}" ${selected ? "selected" : ""}>${size}</option>`;
                    })
                    .join("")}
                </select>
                <div class="pager">
                  <button class="ghost" data-job-page-prev="${section.key}" ${
                    page === 0 || pageSize === Infinity ? "disabled" : ""
                  }>Prev</button>
                  <span class="muted">Page ${page + 1} / ${totalPages}</span>
                  <button class="ghost" data-job-page-next="${section.key}" ${
                    page + 1 >= totalPages || pageSize === Infinity ? "disabled" : ""
                  }>Next</button>
                </div>
              </div>
              <table class="table ${isCollapsed ? "hidden" : ""}">
                <thead>
                  <tr><th>Job</th><th>Type</th><th>Status</th><th>Progress</th><th>Node</th><th>Updated</th><th></th></tr>
                </thead>
                <tbody>
                  ${visibleRows
                    .map((job) => {
                      const normalizedStatus = normalizeJobStatus(job.status);
                      const canReenqueue = normalizedStatus === "error";
                      const typeText = job.type ?? "-";
                      const fileName = getFileName(job.file_path ?? job.new_path ?? "");
                      const jobTitle = fileName || job.file_id || job.id;
                      return `
                        <tr>
                          <td>
                            <div class="job-title">${jobTitle}</div>
                            <div class="muted">${job.id}</div>
                          </td>
                          <td>${typeText}</td>
                          <td>${normalizedStatus}</td>
                          <td>${job.progress ?? 0}%</td>
                          <td>${job.assigned_node_id ?? "-"}</td>
                          <td>${new Date(job.updated_at).toLocaleString()}</td>
                          <td>
                            ${
                              canReenqueue
                                ? `<button class="ghost" data-reenqueue-status="${normalizedStatus}" data-reenqueue-type="${job.type}">Re-enqueue</button>`
                                : ""
                            }
                            <button class="ghost" data-job-logs="${job.id}">Logs</button>
                          </td>
                        </tr>
                      `;
                    })
                    .join("")}
                </tbody>
              </table>
            </div>
          `;
        })
        .join("")}
    </div>
    ${renderLogModal()}
  `;

  const sectionToggles = view.querySelectorAll("button[data-job-section-toggle]");
  sectionToggles.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const key = btn.dataset.jobSectionToggle;
      if (collapsedJobSections.has(key)) {
        collapsedJobSections.delete(key);
      } else {
        collapsedJobSections.add(key);
      }
      await renderJobs();
    });
  });

  const pageSizeSelects = view.querySelectorAll("select[data-job-page-size]");
  pageSizeSelects.forEach((select) => {
    select.addEventListener("change", async () => {
      const key = select.dataset.jobPageSize;
      const value = select.value;
      const size = value === "all" ? Infinity : Number(value);
      jobSectionPageSize.set(key, size);
      jobSectionPage.set(key, 0);
      await renderJobs();
    });
  });

  const prevButtons = view.querySelectorAll("button[data-job-page-prev]");
  prevButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const key = btn.dataset.jobPagePrev;
      const page = jobSectionPage.get(key) ?? 0;
      jobSectionPage.set(key, Math.max(page - 1, 0));
      await renderJobs();
    });
  });

  const nextButtons = view.querySelectorAll("button[data-job-page-next]");
  nextButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const key = btn.dataset.jobPageNext;
      const page = jobSectionPage.get(key) ?? 0;
      jobSectionPage.set(key, page + 1);
      await renderJobs();
    });
  });

  const reenqueueHealthFailed = document.getElementById("reenqueue-health-failed");
  const reenqueueFailed = document.getElementById("reenqueue-transcode-failed");
  const resetJobs = document.getElementById("reset-jobs");

  reenqueueHealthFailed?.addEventListener("click", async () => {
    await fetch("/api/jobs/reenqueue", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status: "error", type: "healthcheck" }),
    });
    await renderJobs();
  });

  reenqueueFailed?.addEventListener("click", async () => {
    await fetch("/api/jobs/reenqueue", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status: "error", type: "transcode" }),
    });
    await renderJobs();
  });

  resetJobs?.addEventListener("click", async () => {
    if (!confirm("This will clear all jobs and reset file statuses. Continue?")) return;
    await fetch("/api/jobs/reset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });
    await renderJobs();
  });

  const reenqueueButtons = view.querySelectorAll("button[data-reenqueue-status]");
  reenqueueButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const status = btn.dataset.reenqueueStatus;
      const type = btn.dataset.reenqueueType;
      await fetch("/api/jobs/reenqueue", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status, type }),
      });
      await renderJobs();
    });
  });

  const logButtons = view.querySelectorAll("button[data-job-logs]");
  logButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const jobId = btn.dataset.jobLogs;
      const job = jobs.find((item) => String(item.id) === String(jobId));
      if (job) openLogModal(job);
    });
  });

  bindLogModal();
}

async function renderTrees() {
  isTreeDesignerActive = false;
  treeDesignerRoot = null;
  const trees = await fetchJson("/api/trees");

  view.innerHTML = `
    <div class="trees-topbar">
      <div>
        <h3>Trees</h3>
        <p class="muted">Select a tree from the sidebar to edit.</p>
      </div>
      <button class="primary" id="tree-create">Create Tree</button>
    </div>
    <div class="card">
      <div class="card-header">
        <div>
          <h3>Tree Designer</h3>
          <p class="muted">Drag nodes and connect branches. Save to create a new version.</p>
        </div>
        <div class="actions">
          <button class="ghost" id="tree-designer-save" disabled>Save</button>
          <select id="tree-actions" class="tree-actions">
            <option value="">Actions</option>
            <option value="delete">Delete tree</option>
          </select>
        </div>
      </div>
      <div id="tree-designer" class="tree-designer">
        <div class="muted">Select a tree from the sidebar to start designing.</div>
      </div>
    </div>
    ${renderTreeModal()}
    ${renderTreeSettingsModal()}
  `;

  document.getElementById("tree-create")?.addEventListener("click", () => {
    openTreeModal();
  });

  bindTreeModal();
  bindTreeSettingsModal();

  document.getElementById("tree-designer-save")?.addEventListener("click", async () => {
    if (typeof treeDesignerState.save === "function") {
      await treeDesignerState.save();
    }
  });

  document.getElementById("tree-actions")?.addEventListener("change", async (event) => {
    const action = event.target.value;
    event.target.value = "";
    if (action === "delete") {
      const treeId = treeDesignerState.treeId;
      if (!treeId) return;
      if (!confirm("Delete this tree? This cannot be undone.")) return;
      await fetch(`/api/trees/${treeId}`, { method: "DELETE" });
      treeDesignerState.treeId = null;
      await renderTrees();
    }
  });

  if (pendingTreeId) {
    const tree = await fetchJson(`/api/trees/${pendingTreeId}`);
    pendingTreeId = null;
    mountTreeDesigner(tree);
  }
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error("Request failed");
  return res.json();
}

function formatBytes(value) {
  if (!value || Number.isNaN(value)) return "-";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(1)}${units[unitIndex]}`;
}

function formatBytesDecimal(value) {
  if (!value || Number.isNaN(value)) return "-";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let unitIndex = 0;
  while (size >= 1000 && unitIndex < units.length - 1) {
    size /= 1000;
    unitIndex += 1;
  }
  return `${size.toFixed(1)}${units[unitIndex]}`;
}

function formatBytesBinary(value) {
  if (!value || Number.isNaN(value)) return "-";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let size = value;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(1)}${units[unitIndex]}`;
}

function formatRamUsage(usedBytes, totalBytes) {
  if (!totalBytes || Number.isNaN(totalBytes)) return "-";
  const gib = 1024 ** 3;
  const mib = 1024 ** 2;
  if (totalBytes < gib) {
    const totalMiB = Math.max(1, Math.round(totalBytes / mib));
    const usedMiB = usedBytes ? (usedBytes / mib).toFixed(1) : "0.0";
    return `${usedMiB}MiB/${totalMiB}MiB`;
  }
  const totalGiB = Math.max(1, Math.round(totalBytes / gib));
  const usedGiB = usedBytes ? (usedBytes / gib).toFixed(1) : "0.0";
  return `${usedGiB}GiB/${totalGiB}GiB`;
}

function getFileName(filePath) {
  if (!filePath) return "";
  const parts = String(filePath).split(/[/\\]/);
  return parts[parts.length - 1] ?? "";
}

function mergeGpuLists(hardwareGpus = [], metricGpus = []) {
  const merged = [...metricGpus.map((gpu) => ({ ...gpu }))];

  hardwareGpus.forEach((gpu) => {
    const match = merged.find((candidate) => matchesGpu(candidate, gpu));
    if (!match) {
      merged.push({ ...gpu });
    } else {
      match.model = match.model ?? gpu.model;
      match.vendor = match.vendor ?? gpu.vendor;
      match.vram = match.vram ?? gpu.vram;
      match.driver = match.driver ?? gpu.driver;
      match.accelerator = match.accelerator ?? gpu.accelerator;
    }
  });

  return merged;
}

function matchesGpu(a, b) {
  const aModel = String(a.model ?? "").toLowerCase();
  const bModel = String(b.model ?? "").toLowerCase();
  const aVendor = String(a.vendor ?? "").toLowerCase();
  const bVendor = String(b.vendor ?? "").toLowerCase();
  if (aModel && bModel) {
    return aModel.includes(bModel) || bModel.includes(aModel);
  }
  return aVendor && bVendor && aVendor.includes(bVendor);
}

function averageGpuUtil(gpus) {
  const utilizations = gpus
    .filter((gpu) => gpu.accelerator !== false)
    .map((gpu) => gpu.utilization)
    .filter((value) => typeof value === "number");
  if (!utilizations.length) return null;
  const total = utilizations.reduce((sum, value) => sum + value, 0);
  return total / utilizations.length;
}

function renderUsageMeter(value, text, hideText = false) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return `<div class="usage-meter" title="${text ?? "-"}"><div class="usage-text">${hideText ? "-" : text ?? "-"}</div></div>`;
  }
  const clamped = Math.max(0, Math.min(100, value));
  const display = hideText ? `${clamped.toFixed(1)}%` : text ?? `${clamped.toFixed(1)}%`;
  return `
    <div class="usage-meter" title="${text ?? display}">
      <div class="usage-text">${display}</div>
      <div class="usage-bar"><div class="usage-fill" style="width:${clamped}%;"></div></div>
    </div>
  `;
}

function normalizeJobStatus(status) {
  if (status === "health_failed" || status === "transcode_failed") return "error";
  if (status === "transcode_successful") return "successful";
  return status ?? "-";
}

function buildJobCounts(stats) {
  const isActiveStatus = (status) => status === "queued" || status === "processing";
  const countBy = (type, predicate) =>
    stats
      .filter((row) => row.type === type)
      .filter((row) => predicate(normalizeJobStatus(row.status)))
      .reduce((sum, row) => sum + Number(row.count ?? 0), 0);

  return {
    healthcheckActive: countBy("healthcheck", isActiveStatus),
    healthcheckError: countBy("healthcheck", (s) => s === "error"),
    transcodeActive: countBy("transcode", isActiveStatus),
    transcodeSuccess: countBy("transcode", (s) => s === "successful"),
    transcodeError: countBy("transcode", (s) => s === "error"),
  };
}


function renderGpuDetailsRow(nodeId, gpus, expanded, lastSeen, ramDetailText) {
  if (!expanded) {
    return `<tr class="gpu-details hidden" data-gpu-details="${nodeId}"></tr>`;
  }

  if (!gpus.length) {
    return `
      <tr class="gpu-details" data-gpu-details="${nodeId}">
        <td colspan="8"><span class="muted">No GPUs reported.</span></td>
      </tr>
    `;
  }

  const rows = gpus
    .map((gpu) => {
      const util = typeof gpu.utilization === "number" ? `${gpu.utilization.toFixed(1)}%` : "-";
      const vramTotalBytes = gpu.vram ? gpu.vram * 1024 * 1024 : null;
      const vramUsedBytes =
        typeof gpu.memoryUtilization === "number" && vramTotalBytes
          ? (gpu.memoryUtilization / 100) * vramTotalBytes
          : null;
      const vramText = vramTotalBytes
        ? `${formatBytes(vramUsedBytes ?? 0)}/${formatBytes(vramTotalBytes)}`
        : "-";
      const vramPercent = typeof gpu.memoryUtilization === "number" ? gpu.memoryUtilization : null;
      const accel = gpu.accelerator === false ? "integrated" : "accelerator";
      return `
        <tr>
          <td>${gpu.model ?? "Unknown"}</td>
          <td>${gpu.vendor ?? "-"}</td>
          <td>${accel}</td>
          <td>${util}</td>
          <td>${renderUsageMeter(vramPercent, vramText)}</td>
        </tr>
      `;
    })
    .join("");

  return `
    <tr class="gpu-details" data-gpu-details="${nodeId}">
      <td colspan="8">
        <div class="gpu-details-meta">
          <div><span class="muted">Last seen:</span> ${new Date(lastSeen).toLocaleString()}</div>
          <div><span class="muted">RAM:</span> ${ramDetailText}</div>
        </div>
        <table class="table subtable">
          <thead>
            <tr><th>Model</th><th>Vendor</th><th>Type</th><th>GPU</th><th>VRAM Usage</th></tr>
          </thead>
          <tbody>
            ${rows}
          </tbody>
        </table>
      </td>
    </tr>
  `;
}

async function startAutoRefresh() {
  const token = ++refreshToken;
  const waitMs = 1500;

  while (token === refreshToken) {
    try {
      if (!isUserEditing()) {
        if (activeView === "trees") {
          await renderSidebarLists();
          await new Promise((resolve) => setTimeout(resolve, waitMs));
          continue;
        }
        const scrollTop = view.scrollTop;
        await renderSidebarLists();
        await routes[activeView]();
        view.scrollTop = scrollTop;
      }
    } catch (err) {
      console.warn("Auto-refresh failed:", err.message ?? err);
    }

    await new Promise((resolve) => setTimeout(resolve, waitMs));
  }
}

async function renderSidebarLists() {
  try {
    const [libraries, nodes, trees] = await Promise.all([
      fetchJson("/api/libraries"),
      fetchJson("/api/nodes"),
      fetchJson("/api/trees"),
    ]);

    const libsContainer = subnavContainers.get("libraries");
    if (libsContainer) {
      libsContainer.innerHTML = libraries
        .map(
          (lib) =>
            `<button class="subnav-item" data-subnav-view="libraries">${lib.name}</button>`
        )
        .join("");
    }

    const nodesContainer = subnavContainers.get("nodes");
    if (nodesContainer) {
      nodesContainer.innerHTML = nodes
        .map(
          (node) =>
            `<button class="subnav-item" data-subnav-view="nodes">${node.name}</button>`
        )
        .join("");
    }

    const jobsContainer = subnavContainers.get("jobs");
    if (jobsContainer) {
      const queues = [
        "Healthcheck",
        "Health Failed",
        "Transcode",
        "Transcode Successful",
        "Transcode Failed",
      ];
      jobsContainer.innerHTML = queues
        .map((queue) => `<button class="subnav-item" data-subnav-view="jobs">${queue}</button>`)
        .join("");
    }

    const treesContainer = subnavContainers.get("trees");
    if (treesContainer) {
      treesContainer.innerHTML = trees
        .map(
          (tree) =>
            `<button class="subnav-item" data-subnav-view="trees" data-tree-id="${tree.id}">${tree.name}</button>`
        )
        .join("");
    }
  } catch {
    // ignore sidebar refresh errors
  }
}

function isUserEditing() {
  const active = document.activeElement;
  if (!active) return false;
  if (
    isLibraryModalOpen ||
    isLogModalOpen ||
    isTreeModalOpen ||
    isTreeAssignModalOpen ||
    isTreeDesignerActive
  ) {
    return true;
  }
  const tag = active.tagName?.toLowerCase();
  if (tag === "input" || tag === "textarea" || tag === "select") {
    return view.contains(active);
  }
  return false;
}

function renderLibraryDetailsRow(lib, expanded) {
  if (!expanded) {
    return `<tr class="library-details hidden" data-library-details="${lib.id}"></tr>`;
  }

  const page = libraryFilesPage.get(lib.id) ?? 0;
  const offset = page * 25;
  return `
    <tr class="library-details" data-library-details="${lib.id}">
      <td colspan="5">
        <div class="library-files" data-library-files="${lib.id}" data-offset="${offset}">
          <div class="muted">Loading files...</div>
        </div>
      </td>
    </tr>
  `;
}

async function renderLibraryFiles(libId, offset) {
  const container = view.querySelector(`[data-library-files='${libId}']`);
  if (!container) return;

  const data = await fetchJson(`/api/libraries/${libId}/files?offset=${offset}&limit=25`);
  const total = data.total ?? 0;
  const files = data.files ?? [];
  const page = Math.floor(offset / 25);
  const totalPages = Math.max(Math.ceil(total / 25), 1);

  container.innerHTML = `
    <div class="library-files-header">
      <div class="muted">Showing ${offset + 1}-${Math.min(offset + 25, total)} of ${total}</div>
      <div class="pager">
        <button class="ghost" data-page-prev="${libId}" ${page === 0 ? "disabled" : ""}>Prev</button>
        <span class="muted">Page ${page + 1} / ${totalPages}</span>
        <button class="ghost" data-page-next="${libId}" ${page + 1 >= totalPages ? "disabled" : ""}>Next</button>
      </div>
    </div>
    <table class="table subtable">
      <thead>
        <tr><th>Name</th><th>Status</th><th>Initial</th><th>Final</th><th>New Name</th></tr>
      </thead>
      <tbody>
        ${files
          .map((file) => {
            const name = file.path ? file.path.split(/[/\\]/).pop() : "-";
            const initialSize = formatBytes(file.initial_size ?? file.size ?? 0);
            const initialContainer = file.initial_container ?? "-";
            const initialCodec = file.initial_codec ?? "-";
            const finalSize = file.final_size ? formatBytes(file.final_size) : "-";
            const finalContainer = file.final_container ?? "-";
            const finalCodec = file.final_codec ?? "-";
            const newName = file.new_path ? file.new_path.split(/[/\\]/).pop() : "-";
            const initialText = `${initialSize} · ${initialContainer} · ${initialCodec}`;
            const finalText = `${finalSize} · ${finalContainer} · ${finalCodec}`;
            return `
              <tr>
                <td title="${file.path ?? ""}">${name}</td>
                <td>${file.status}</td>
                <td>${initialText}</td>
                <td>${finalText}</td>
                <td title="${file.new_path ?? ""}">${newName}</td>
              </tr>
            `;
          })
          .join("")}
      </tbody>
    </table>
  `;

  const prevBtn = container.querySelector("button[data-page-prev]");
  const nextBtn = container.querySelector("button[data-page-next]");
  prevBtn?.addEventListener("click", async () => {
    const nextOffset = Math.max(offset - 25, 0);
    libraryFilesPage.set(libId, Math.max(page - 1, 0));
    await renderLibraryFiles(libId, nextOffset);
  });
  nextBtn?.addEventListener("click", async () => {
    const nextOffset = offset + 25;
    libraryFilesPage.set(libId, page + 1);
    await renderLibraryFiles(libId, nextOffset);
  });
}

function renderLibraryModal(nodeNames) {
  return `
    <div class="modal-backdrop hidden" id="library-modal-backdrop">
      <div class="modal">
        <div class="modal-header">
          <h3 id="library-modal-title">Add Library</h3>
          <button class="ghost" id="library-modal-close">Close</button>
        </div>
        <form class="form" id="library-form">
          <input name="name" placeholder="Library name" required />
          <input name="path" placeholder="Path (e.g. D:\\Media)" required />
          <input name="includeExtensions" placeholder="Include extensions (csv)" />
          <input name="excludeExtensions" placeholder="Exclude extensions (csv)" />
          <input name="nodes" placeholder="Allowed nodes (csv)" list="node-names" />
          <input name="scanIntervalMin" placeholder="Scan interval minutes (default 15)" />
          <datalist id="node-names">
            ${nodeNames.map((name) => `<option value="${name}"></option>`).join("")}
          </datalist>
          <input type="hidden" name="libraryId" />
          <button class="primary" type="submit">Save Library</button>
        </form>
      </div>
    </div>
  `;
}

function openLibraryModal(library) {
  const backdrop = document.getElementById("library-modal-backdrop");
  if (!backdrop) return;
  backdrop.classList.remove("hidden");
  isLibraryModalOpen = true;

  const title = document.getElementById("library-modal-title");
  title.textContent = library ? "Edit Library" : "Add Library";

  const form = document.getElementById("library-form");
  form.reset();
  form.elements.libraryId.value = library?.id ?? "";
  form.elements.name.value = library?.name ?? "";
  form.elements.path.value = library?.path ?? "";
  form.elements.includeExtensions.value = parseJsonList(library?.include_exts).join(", ");
  form.elements.excludeExtensions.value = parseJsonList(library?.exclude_exts).join(", ");
  form.elements.nodes.value = parseJsonList(library?.nodes_json).join(", ");
  form.elements.scanIntervalMin.value = library?.scan_interval_min ?? "";
}

function closeLibraryModal() {
  const backdrop = document.getElementById("library-modal-backdrop");
  if (!backdrop) return;
  backdrop.classList.add("hidden");
  isLibraryModalOpen = false;
}

function renderLogModal() {
  return `
    <div class="modal-backdrop hidden" id="log-modal-backdrop">
      <div class="modal log-modal">
        <div class="modal-header">
          <h3 id="log-modal-title">Job Logs</h3>
          <button class="ghost" id="log-modal-close">Close</button>
        </div>
        <div class="log-meta" id="log-meta"></div>
        <div class="log-layout">
          <div class="log-list" id="log-list"></div>
          <div class="log-content" id="log-content">
            <div class="muted">Select a log entry.</div>
          </div>
        </div>
      </div>
    </div>
  `;
}

function parseJobLogEntries(job) {
  const raw = job?.log_text ?? "";
  if (!raw.trim()) {
    return [
      {
        id: 0,
        stage: job?.type ?? "unknown",
        ts: null,
        message: "(no log)",
        order: 0,
      },
    ];
  }

  const lines = raw.split("\n").filter((line) => line.trim().length);
  const entries = [];
  const legacyLines = [];

  lines.forEach((line, index) => {
    try {
      const parsed = JSON.parse(line);
      entries.push({
        id: entries.length,
        stage: parsed.stage ?? job?.type ?? "unknown",
        ts: parsed.ts ?? null,
        message: parsed.message ?? "",
        order: index,
      });
    } catch {
      legacyLines.push(line);
    }
  });

  if (!entries.length) {
    return [
      {
        id: 0,
        stage: job?.type ?? "unknown",
        ts: null,
        message: raw,
        order: 0,
      },
    ];
  }

  if (legacyLines.length) {
    entries.push({
      id: entries.length,
      stage: job?.type ?? "legacy",
      ts: null,
      message: legacyLines.join("\n"),
      order: lines.length + 1,
    });
  }

  return entries;
}

function formatLogTimestamp(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString();
}

function openLogModal(job) {
  const backdrop = document.getElementById("log-modal-backdrop");
  if (!backdrop) return;

  currentLogJobId = job?.id ?? null;
  currentLogEntries = parseJobLogEntries(job)
    .map((entry, index) => ({ ...entry, id: index }))
    .sort((a, b) => {
      const aTime = a.ts ? new Date(a.ts).getTime() : -Infinity;
      const bTime = b.ts ? new Date(b.ts).getTime() : -Infinity;
      if (aTime === bTime) return b.order - a.order;
      return bTime - aTime;
    });

  const title = document.getElementById("log-modal-title");
  if (title) title.textContent = `Job ${job.id} logs`;

  const meta = document.getElementById("log-meta");
  if (meta) {
    const fileName = getFileName(job.file_path ?? job.new_path ?? "") || "-";
    const initialSize = formatBytes(job.initial_size ?? job.file_size ?? 0);
    const initialContainer = job.initial_container ?? "-";
    const initialCodec = job.initial_codec ?? "-";
    const finalSize = job.final_size ? formatBytes(job.final_size) : "-";
    const finalContainer = job.final_container ?? "-";
    const finalCodec = job.final_codec ?? "-";
    const newName = getFileName(job.new_path ?? "") || "-";

    meta.innerHTML = `
      <div class="log-meta-grid">
        <div>
          <div class="muted">Job ID</div>
          <div>${job.id}</div>
        </div>
        <div>
          <div class="muted">File</div>
          <div title="${job.file_path ?? ""}">${fileName}</div>
        </div>
        <div>
          <div class="muted">Type</div>
          <div>${job.type ?? "-"}</div>
        </div>
        <div>
          <div class="muted">Status</div>
          <div>${normalizeJobStatus(job.status)}</div>
        </div>
        <div>
          <div class="muted">Initial</div>
          <div>${initialSize} · ${initialContainer} · ${initialCodec}</div>
        </div>
        <div>
          <div class="muted">Final</div>
          <div>${finalSize} · ${finalContainer} · ${finalCodec}</div>
        </div>
        <div>
          <div class="muted">New Name</div>
          <div title="${job.new_path ?? ""}">${newName}</div>
        </div>
      </div>
    `;
  }

  const list = document.getElementById("log-list");
  const content = document.getElementById("log-content");
  if (list) {
    list.innerHTML = currentLogEntries
      .map((entry, index) => {
        const tsText = formatLogTimestamp(entry.ts);
        const label = entry.stage ?? "unknown";
        return `
          <button class="log-item ${index === 0 ? "active" : ""}" data-log-index="${index}">
            <div class="log-item-title">${label}</div>
            <div class="muted log-item-sub">${tsText || ""}</div>
          </button>
        `;
      })
      .join("");
  }

  if (content) {
    const first = currentLogEntries[0];
    content.textContent = first?.message ?? "(no log)";
  }

  backdrop.classList.remove("hidden");
  isLogModalOpen = true;
}

function closeLogModal() {
  const backdrop = document.getElementById("log-modal-backdrop");
  if (!backdrop) return;
  backdrop.classList.add("hidden");
  isLogModalOpen = false;
  currentLogJobId = null;
  currentLogEntries = [];
}

function bindLogModal() {
  const backdrop = document.getElementById("log-modal-backdrop");
  const closeBtn = document.getElementById("log-modal-close");
  const list = document.getElementById("log-list");
  const content = document.getElementById("log-content");

  closeBtn?.addEventListener("click", (event) => {
    event.preventDefault();
    closeLogModal();
  });

  backdrop?.addEventListener("click", (event) => {
    if (event.target === backdrop) closeLogModal();
  });

  list?.addEventListener("click", (event) => {
    const target = event.target.closest("button[data-log-index]");
    if (!target) return;
    const index = Number(target.dataset.logIndex);
    const entry = currentLogEntries[index];
    if (!entry || !content) return;

    list.querySelectorAll(".log-item").forEach((item) => {
      item.classList.toggle("active", item === target);
    });

    content.textContent = entry.message ?? "(no log)";
  });
}

function bindLibraryModal(libs) {
  const backdrop = document.getElementById("library-modal-backdrop");
  const closeBtn = document.getElementById("library-modal-close");
  const form = document.getElementById("library-form");

  closeBtn?.addEventListener("click", (event) => {
    event.preventDefault();
    closeLibraryModal();
  });
  backdrop?.addEventListener("click", (event) => {
    if (event.target === backdrop) closeLibraryModal();
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(form).entries());
    const includeExtensions = data.includeExtensions
      ? data.includeExtensions.split(/[,\s]+/).filter(Boolean)
      : undefined;
    const excludeExtensions = data.excludeExtensions
      ? data.excludeExtensions.split(/[,\s]+/).filter(Boolean)
      : undefined;
    const nodes = data.nodes ? data.nodes.split(/[,\s]+/).filter(Boolean) : undefined;
    const scanIntervalMin = data.scanIntervalMin ? Number(data.scanIntervalMin) : undefined;

    const payload = {
      name: data.name,
      path: data.path,
      includeExtensions,
      excludeExtensions,
      nodes,
      scanIntervalMin,
    };

    if (data.libraryId) {
      await fetch(`/api/libraries/${data.libraryId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
    } else {
      await fetch("/api/libraries", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
    }

    closeLibraryModal();
    await renderLibraries();
  });

  const expanded = Array.from(expandedLibraries);
  for (const libId of expanded) {
    const page = libraryFilesPage.get(libId) ?? 0;
    renderLibraryFiles(libId, page * 25);
  }
}

function renderTreeModal() {
  return `
    <div class="modal-backdrop hidden" id="tree-modal-backdrop">
      <div class="modal tree-modal">
        <div class="modal-header">
          <h3 id="tree-modal-title">Create Tree</h3>
          <button class="ghost" id="tree-modal-close">Close</button>
        </div>
        <form class="form" id="tree-form">
          <input name="name" placeholder="Tree name" required />
          <textarea name="description" placeholder="Description"></textarea>
          <input type="hidden" name="treeId" />
          <button class="primary" type="submit">Save</button>
        </form>
      </div>
    </div>
  `;
}

function renderTreeAssignModal(libraries) {
  return `
    <div class="modal-backdrop hidden" id="tree-assign-backdrop">
      <div class="modal">
        <div class="modal-header">
          <h3 id="tree-assign-title">Assign Tree</h3>
          <button class="ghost" id="tree-assign-close">Close</button>
        </div>
        <form class="form" id="tree-assign-form">
          <select name="libraryId" required>
            <option value="">Select library...</option>
            ${libraries
              .map((lib) => `<option value="${lib.id}">${lib.name}</option>`)
              .join("")}
          </select>
          <input name="treeId" type="hidden" />
          <input name="treeVersion" placeholder="Tree version (e.g. 1)" required />
          <button class="primary" type="submit">Assign</button>
        </form>
      </div>
    </div>
  `;
}

function renderTreeSettingsModal() {
  return `
    <div class="modal-backdrop hidden" id="tree-settings-backdrop">
      <div class="modal">
        <div class="modal-header">
          <h3 id="tree-settings-title">Node Settings</h3>
          <button class="ghost" id="tree-settings-close">Close</button>
        </div>
        <div class="tree-settings-info" id="tree-settings-info"></div>
        <div class="tree-settings-error hidden" id="tree-settings-error"></div>
        <form class="form" id="tree-settings-form"></form>
      </div>
    </div>
  `;
}

function openTreeSettingsModal(node) {
  const backdrop = document.getElementById("tree-settings-backdrop");
  const form = document.getElementById("tree-settings-form");
  const title = document.getElementById("tree-settings-title");
  const info = document.getElementById("tree-settings-info");
  const error = document.getElementById("tree-settings-error");
  if (!backdrop || !form || !title) return;

  const elementType = node?.data?.elementType ?? "unknown";
  const def = getElementDef(elementType);
  title.textContent = `Settings: ${node?.data?.label ?? elementType}`;
  form.dataset.nodeId = node.id;

  const config = node?.data?.config ?? {};
  form.innerHTML = buildSettingsFields(def, config);

  if (info) {
    const sourceLabel = def?.source === "plugin" ? `Plugin (${def?.plugin ?? "custom"})` : "Built-in";
    info.innerHTML = `
      <div><strong>Name</strong> ${def?.label ?? elementType}</div>
      <div><strong>Type</strong> ${elementType}</div>
      <div><strong>Source</strong> ${sourceLabel}</div>
      <div><strong>Description</strong> ${def?.description ?? "No description."}</div>
      <div><strong>Usage</strong> ${def?.usage ?? "No usage notes."}</div>
    `;
  }

  if (error) {
    error.textContent = "";
    error.classList.add("hidden");
  }

  backdrop.classList.remove("hidden");
  isTreeModalOpen = true;
}

function closeTreeSettingsModal() {
  const backdrop = document.getElementById("tree-settings-backdrop");
  if (!backdrop) return;
  backdrop.classList.add("hidden");
  isTreeModalOpen = false;
}

function bindTreeSettingsModal() {
  const backdrop = document.getElementById("tree-settings-backdrop");
  const closeBtn = document.getElementById("tree-settings-close");
  const form = document.getElementById("tree-settings-form");

  closeBtn?.addEventListener("click", (event) => {
    event.preventDefault();
    closeTreeSettingsModal();
  });

  backdrop?.addEventListener("click", (event) => {
    if (event.target === backdrop) closeTreeSettingsModal();
  });

  form?.addEventListener("submit", (event) => {
    event.preventDefault();
    const nodeId = form.dataset.nodeId;
    if (!nodeId || typeof treeDesignerState.updateNodeConfig !== "function") return;
    const data = Object.fromEntries(new FormData(form).entries());
    const elementType = data.elementType;
    const def = getElementDef(elementType);
    const { config, errors } = parseSettingsConfig(def, data);
    const error = document.getElementById("tree-settings-error");
    if (errors.length) {
      if (error) {
        error.innerHTML = errors.map((item) => `<div>${item}</div>`).join("");
        error.classList.remove("hidden");
      }
      return;
    }
    if (error) {
      error.textContent = "";
      error.classList.add("hidden");
    }
    treeDesignerState.updateNodeConfig(nodeId, config);
    closeTreeSettingsModal();
  });
}

function buildSettingsFields(def, config) {
  const fields = [];
  const elementType = def?.type ?? "unknown";
  fields.push(`<input type="hidden" name="elementType" value="${elementType}" />`);

  const fieldDefs = def?.fields ?? [];
  fieldDefs.forEach((field) => {
    const label = field.label ?? field.key;
    const name = field.key;
    const placeholder = field.placeholder ?? "";
    const type = field.type ?? "text";
    const current = getConfigValue(config, field.path ?? field.key, field.default);
    const value = type === "json"
      ? JSON.stringify(current ?? field.default ?? {}, null, 2)
      : type === "textarea"
        ? Array.isArray(current) && field.format === "lines"
          ? current.join("\n")
          : current ?? ""
        : current ?? "";
    fields.push(`<label class="muted">${label}</label>`);
    if (field.description) {
      fields.push(`<div class="muted" style="font-size:12px;">${field.description}</div>`);
    }
    if (type === "textarea" || type === "json") {
      fields.push(
        `<textarea name="${name}" placeholder="${placeholder}" rows="4">${value ?? ""}</textarea>`
      );
    } else {
      const inputType = type === "number" ? "number" : "text";
      fields.push(
        `<input name="${name}" type="${inputType}" value="${value ?? ""}" placeholder="${placeholder}" />`
      );
    }
  });

  if (!fieldDefs.length) {
    fields.push(`<div class="muted">No settings for this element.</div>`);
  }

  fields.push(`<button class="primary" type="submit">Save settings</button>`);
  return fields.join("");
}

function parseSettingsConfig(def, data) {
  const config = {};
  const errors = [];
  const fieldDefs = def?.fields ?? [];

  fieldDefs.forEach((field) => {
    const key = field.key;
    const raw = data[key];
    const label = field.label ?? key;
    const type = field.type ?? "text";
    const path = field.path ?? key;

    if (field.regex && raw) {
      const re = new RegExp(field.regex);
      if (!re.test(raw)) {
        errors.push(`${label}: invalid format.`);
        return;
      }
    }

    if (raw === undefined || raw === "") {
      if (field.default !== undefined) {
        setDeepValue(config, path, field.default);
      }
      return;
    }

    if (type === "number") {
      const num = Number(raw);
      if (Number.isNaN(num)) {
        errors.push(`${label}: must be a number.`);
        return;
      }
      setDeepValue(config, path, num);
      return;
    }

    if (type === "json") {
      try {
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          errors.push(`${label}: must be a JSON object.`);
          return;
        }
        setDeepValue(config, path, parsed);
      } catch {
        errors.push(`${label}: invalid JSON.`);
      }
      return;
    }

    if (type === "textarea") {
      if (field.format === "lines") {
        setDeepValue(
          config,
          path,
          String(raw)
            .split(/\r?\n/)
            .map((line) => line.trim())
            .filter(Boolean)
        );
      } else {
        setDeepValue(config, path, String(raw));
      }
      return;
    }

    if (field.format === "csv") {
      setDeepValue(
        config,
        path,
        String(raw)
          .split(/[\s,]+/)
          .map((value) => value.trim())
          .filter(Boolean)
      );
      return;
    }

    setDeepValue(config, path, String(raw));
  });

  return { config, errors };
}

function getConfigValue(config, path, fallback) {
  if (!config) return fallback;
  if (!path) return fallback;
  const segments = path.split(".");
  let current = config;
  for (const segment of segments) {
    if (current && typeof current === "object" && segment in current) {
      current = current[segment];
    } else {
      return fallback;
    }
  }
  return current ?? fallback;
}

function setDeepValue(target, path, value) {
  if (!path) return;
  const segments = path.split(".");
  let current = target;
  segments.forEach((segment, index) => {
    if (index === segments.length - 1) {
      current[segment] = value;
      return;
    }
    if (!current[segment] || typeof current[segment] !== "object") {
      current[segment] = {};
    }
    current = current[segment];
  });
}

function getElementDef(elementType) {
  return TREE_ELEMENT_DEFS.find((item) => item.type === elementType);
}

function openTreeModal(tree) {
  const backdrop = document.getElementById("tree-modal-backdrop");
  if (!backdrop) return;
  backdrop.classList.remove("hidden");
  isTreeModalOpen = true;
  const title = document.getElementById("tree-modal-title");
  const form = document.getElementById("tree-form");
  if (!form) return;

  title.textContent = tree ? `Edit Tree` : "Create Tree";
  form.reset();
  form.elements.treeId.value = tree?.id ?? "";
  form.elements.name.value = tree?.name ?? "";
  form.elements.description.value = tree?.description ?? "";
}

function closeTreeModal() {
  const backdrop = document.getElementById("tree-modal-backdrop");
  if (!backdrop) return;
  backdrop.classList.add("hidden");
  isTreeModalOpen = false;
}

function bindTreeModal() {
  const backdrop = document.getElementById("tree-modal-backdrop");
  const closeBtn = document.getElementById("tree-modal-close");
  const form = document.getElementById("tree-form");

  closeBtn?.addEventListener("click", (event) => {
    event.preventDefault();
    closeTreeModal();
  });

  backdrop?.addEventListener("click", (event) => {
    if (event.target === backdrop) closeTreeModal();
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(form).entries());
    if (data.treeId) {
      await fetch(`/api/trees/${data.treeId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: data.name, description: data.description }),
      });
    } else {
      await fetch("/api/trees", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: data.name, description: data.description }),
      });
    }

    closeTreeModal();
    await renderTrees();
  });
}

function openTreeAssignModal(tree, libraries) {
  const backdrop = document.getElementById("tree-assign-backdrop");
  if (!backdrop) return;
  backdrop.classList.remove("hidden");
  isTreeAssignModalOpen = true;
  const title = document.getElementById("tree-assign-title");
  const form = document.getElementById("tree-assign-form");
  if (!form) return;

  title.textContent = `Assign Tree: ${tree?.name ?? ""}`;
  form.reset();
  form.elements.treeId.value = tree?.id ?? "";
  form.elements.treeVersion.value = tree?.latestVersion ?? tree?.latest_version ?? "";
}

function closeTreeAssignModal() {
  const backdrop = document.getElementById("tree-assign-backdrop");
  if (!backdrop) return;
  backdrop.classList.add("hidden");
  isTreeAssignModalOpen = false;
}

function bindTreeAssignModal(libraries) {
  const backdrop = document.getElementById("tree-assign-backdrop");
  const closeBtn = document.getElementById("tree-assign-close");
  const form = document.getElementById("tree-assign-form");

  closeBtn?.addEventListener("click", (event) => {
    event.preventDefault();
    closeTreeAssignModal();
  });

  backdrop?.addEventListener("click", (event) => {
    if (event.target === backdrop) closeTreeAssignModal();
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(form).entries());
    const treeVersion = Number(data.treeVersion);
    if (!treeVersion || Number.isNaN(treeVersion)) {
      alert("Tree version must be a number.");
      return;
    }

    await fetch(`/api/libraries/${data.libraryId}/tree`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ treeId: data.treeId, treeVersion }),
    });

    closeTreeAssignModal();
    await renderTrees();
  });
}

function mountTreeDesigner(tree) {
  const container = document.getElementById("tree-designer");
  if (!container || !window.React || !window.ReactDOM || !window.ReactFlow) return;

  isTreeDesignerActive = true;
  treeDesignerState.treeId = tree?.id ?? null;

  const React = window.React;
  const ReactDOM = window.ReactDOM;
  const ReactFlow = window.ReactFlow.ReactFlow;
  const Background = window.ReactFlow.Background;
  const Controls = window.ReactFlow.Controls;
  const MiniMap = window.ReactFlow.MiniMap;
  const addEdge = window.ReactFlow.addEdge;
  const useNodesState = window.ReactFlow.useNodesState;
  const useEdgesState = window.ReactFlow.useEdgesState;
  const useReactFlow = window.ReactFlow.useReactFlow;
  const Handle = window.ReactFlow.Handle;
  const Position = window.ReactFlow.Position;
  const ReactFlowProvider = window.ReactFlow.ReactFlowProvider;

  const initialGraph = tree?.graph ? JSON.parse(tree.graph) : null;
  const initialNodes = (initialGraph?.nodes ?? []).map((node, index) =>
    normalizeElementNode(
      {
        ...node,
        position:
          node?.position && typeof node.position.x === "number" && typeof node.position.y === "number"
            ? node.position
            : { x: 80 + index * 40, y: 80 + index * 40 },
      },
      TREE_ELEMENT_DEFS
    )
  );
  const initialEdges = initialGraph?.edges ?? [];

  const TreeNode = ({ data, id }) => {
    const outputs = data?.outputs ?? [];
    const elementType = data?.elementType;
    return React.createElement(
      "div",
      {
        className: "tree-node",
        onDoubleClick: () => openTreeSettingsModal({ id, data }),
      },
      React.createElement("div", { className: "tree-node-title" }, data?.label ?? "Node"),
      React.createElement(
        "div",
        { className: "tree-node-sub" },
        data?.elementType ?? "custom"
      ),
      elementType === "input"
        ? null
        : React.createElement(Handle, { type: "target", position: Position.Top, id: "in" }),
      outputs.map((output, index) =>
        React.createElement(Handle, {
          key: output.id,
          type: "source",
          position: Position.Bottom,
          id: output.id,
          style: { left: 12 + index * 18 },
        })
      ),
      React.createElement(Handle, {
        type: "source",
        position: Position.Right,
        id: "default",
        className: "tree-node-default-handle",
      })
    );
  };

  const Designer = () => {
    const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
    const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
    const [dirty, setDirty] = React.useState(false);
    const { project } = useReactFlow();

    React.useEffect(() => {
      setNodes(initialNodes);
      setEdges(initialEdges);
      setDirty(false);
    }, [tree?.id]);

    const onNodesChangeWithDirty = React.useCallback(
      (changes) => {
        onNodesChange(changes);
        setDirty(true);
      },
      [onNodesChange]
    );

    const onEdgesChangeWithDirty = React.useCallback(
      (changes) => {
        onEdgesChange(changes);
        setDirty(true);
      },
      [onEdgesChange]
    );

    const onConnect = React.useCallback(
      (params) => {
        setEdges((eds) => addEdge(params, eds));
        setDirty(true);
      },
      []
    );

    const addNode = (elementType, position) => {
      const def = TREE_ELEMENT_DEFS.find((item) => item.type === elementType);
      if (!def) return;
      if (elementType === "input" && nodes.some((node) => node.data?.elementType === "input")) {
        alert("Only one input element is allowed in a tree.");
        return;
      }
      const id = `${elementType}-${Date.now()}`;
      setNodes((nds) => [
        ...nds,
        {
          id,
          type: "treeNode",
          position: position ?? { x: 50 + nds.length * 40, y: 50 + nds.length * 40 },
          data: {
            label: def.label,
            elementType: def.type,
            outputs: def.outputs,
            context: {},
          },
        },
      ]);
      setDirty(true);
    };

    const onDrop = React.useCallback(
      (event) => {
        event.preventDefault();
        const elementType = event.dataTransfer.getData("application/reactflow");
        if (!elementType) return;
        const bounds = event.currentTarget.getBoundingClientRect();
        const position = project({
          x: event.clientX - bounds.left,
          y: event.clientY - bounds.top,
        });
        addNode(elementType, position);
      },
      [project]
    );

    const onDragOver = React.useCallback((event) => {
      event.preventDefault();
      event.dataTransfer.dropEffect = "move";
    }, []);

    const onNodeDoubleClick = React.useCallback((event, node) => {
      openTreeSettingsModal(node);
    }, []);

    const onNodeClick = React.useCallback((event, node) => {
      if (event.detail === 2) {
        openTreeSettingsModal(node);
      }
    }, []);

    const updateNodeConfig = React.useCallback(
      (nodeId, config) => {
        setNodes((nds) =>
          nds.map((n) =>
            n.id === nodeId
              ? {
                  ...n,
                  data: {
                    ...(n.data ?? {}),
                    config,
                  },
                }
              : n
          )
        );
        setDirty(true);
      },
      [setNodes]
    );

    const save = async () => {
      if (!tree?.id) return;
      const graph = { version: 1, nodes, edges };
      await fetch(`/api/trees/${tree.id}/versions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ graph }),
      });
      setDirty(false);
      await renderTrees();
    };

    React.useEffect(() => {
      treeDesignerState.treeId = tree?.id ?? null;
      treeDesignerState.save = save;
      treeDesignerState.isDirty = dirty;
      treeDesignerState.updateNodeConfig = updateNodeConfig;
      const saveBtn = document.getElementById("tree-designer-save");
      if (saveBtn) {
        saveBtn.disabled = !treeDesignerState.treeId || !dirty;
        saveBtn.textContent = dirty ? "Save changes" : "Saved";
      }
    }, [dirty, tree?.id]);

    return React.createElement(
      "div",
      { className: "tree-designer-inner" },
      React.createElement(
        "div",
        { className: "tree-designer-toolbar" },
        React.createElement("div", { className: "muted" }, tree?.name ?? ""),
        React.createElement("div", { className: "toolbar-actions" })
      ),
      React.createElement(
        "div",
        { className: "tree-designer-body" },
        React.createElement(
          "div",
          { className: "tree-designer-canvas" },
          React.createElement(
            ReactFlow,
            {
              nodes,
              edges,
              onNodesChange: onNodesChangeWithDirty,
              onEdgesChange: onEdgesChangeWithDirty,
              onConnect,
              onDrop,
              onDragOver,
              onNodeDoubleClick,
              onNodeClick,
              nodeTypes: { treeNode: TreeNode },
              fitView: true,
            },
            React.createElement(Background, { gap: 16 }),
            React.createElement(Controls),
            React.createElement(MiniMap)
          )
        ),
        React.createElement(
          "div",
          { className: "tree-designer-palette" },
          TREE_ELEMENT_DEFS.map((item) =>
            React.createElement(
              "div",
              {
                key: item.type,
                className: "tree-palette-item",
                draggable: true,
                onDragStart: (event) => {
                  event.dataTransfer.setData("application/reactflow", item.type);
                  event.dataTransfer.effectAllowed = "move";
                },
              },
              item.label
            )
          )
        )
      )
    );
  };

  if (!treeDesignerRoot) {
    treeDesignerRoot = ReactDOM.createRoot(container);
  }
  treeDesignerRoot.render(
    React.createElement(ReactFlowProvider, null, React.createElement(Designer))
  );
}


function parseJsonList(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

checkHealth();
renderSidebarLists();
renderDashboard();
startAutoRefresh();
