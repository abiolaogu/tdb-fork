'use client';

import { useEffect, useState } from 'react';

export default function Home() {
  const [stats, setStats] = useState({
    collections: "12",
    documents: "1.2M",
    events_processed: "85k/s",
    read_latency: "0.8ms",
    nodes: 3,
  });

  const statCards = [
    { title: "Total Collections", value: stats.collections, change: "+2", color: "from-blue-500 to-blue-600" },
    { title: "Documents Stored", value: stats.documents, change: "+15%", color: "from-purple-500 to-purple-600" },
    { title: "Event Throughput", value: stats.events_processed, change: "+5.2k", color: "from-green-500 to-green-600" },
    { title: "P99 Latency", value: stats.read_latency, change: "-0.1ms", color: "from-orange-500 to-red-500" },
  ];

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex justify-between items-end">
        <div>
          <h2 className="text-3xl font-bold text-white tracking-tight">Dashboard</h2>
          <p className="text-gray-400 mt-1">Overview of your LumaDB cluster.</p>
        </div>
        <div className="flex space-x-3">
          <button className="px-4 py-2 bg-gray-800 hover:bg-gray-700 text-white rounded-lg text-sm font-medium transition-colors">
            Refresh
          </button>
          <button className="px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white rounded-lg text-sm font-medium transition-colors">
            New Collection
          </button>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {statCards.map((stat) => (
          <div key={stat.title} className="bg-gray-900/50 backdrop-blur-xl p-6 rounded-2xl border border-gray-800/50 shadow-xl hover:border-gray-700 transition-all">
            <div className="flex justify-between items-start mb-4">
              <h3 className="text-gray-400 text-sm font-medium">{stat.title}</h3>
              <div className={`w-2 h-2 rounded-full bg-gradient-to-tr ${stat.color}`}></div>
            </div>
            <div className="flex items-baseline space-x-3">
              <span className="text-3xl font-bold text-white tracking-tight">{stat.value}</span>
              <span className="text-xs text-green-400 font-medium bg-green-400/10 px-2 py-0.5 rounded-full">{stat.change}</span>
            </div>
          </div>
        ))}
      </div>

      {/* Main Content Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Chart Section */}
        <div className="lg:col-span-2 bg-gray-900/50 p-6 rounded-2xl border border-gray-800/50 h-96 flex flex-col">
          <h3 className="text-lg font-semibold text-white mb-6">Throughput History</h3>
          <div className="flex-1 flex items-end justify-between space-x-2 px-2 pb-2">
            {[30, 45, 35, 60, 55, 70, 65, 80, 75, 90, 85, 95].map((h, i) => (
              <div key={i} className="w-full bg-blue-600/20 rounded-t-sm hover:bg-blue-600/40 transition-colors relative group">
                <div style={{ height: `${h}%` }} className="absolute bottom-0 w-full bg-blue-600 rounded-t-sm opacity-60 group-hover:opacity-100 transition-opacity"></div>
              </div>
            ))}
          </div>
          <div className="flex justify-between text-xs text-gray-500 mt-4 px-2">
            <span>00:00</span>
            <span>12:00</span>
          </div>
        </div>

        {/* Health & Status */}
        <div className="bg-gray-900/50 p-6 rounded-2xl border border-gray-800/50 h-96 overflow-y-auto">
          <h3 className="text-lg font-semibold text-white mb-4">Cluster Health</h3>
          <div className="space-y-4">
            <div className="p-4 bg-gray-800/30 rounded-xl border border-gray-700/50 flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
                <div>
                  <p className="text-sm font-medium text-white">Placement Driver</p>
                  <p className="text-xs text-gray-400">Leader (Node 1)</p>
                </div>
              </div>
              <span className="text-xs text-green-400 font-mono">OK</span>
            </div>

            <div className="p-4 bg-gray-800/30 rounded-xl border border-gray-700/50 flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                <div>
                  <p className="text-sm font-medium text-white">Store Node 1</p>
                  <p className="text-xs text-gray-400">192.168.1.10</p>
                </div>
              </div>
              <span className="text-xs text-green-400 font-mono">OK</span>
            </div>

            <div className="p-4 bg-gray-800/30 rounded-xl border border-gray-700/50 flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                <div>
                  <p className="text-sm font-medium text-white">Store Node 2</p>
                  <p className="text-xs text-gray-400">192.168.1.11</p>
                </div>
              </div>
              <span className="text-xs text-green-400 font-mono">OK</span>
            </div>

            <div className="p-4 bg-gray-800/30 rounded-xl border border-gray-700/50 flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                <div>
                  <p className="text-sm font-medium text-white">Store Node 3</p>
                  <p className="text-xs text-gray-400">192.168.1.12</p>
                </div>
              </div>
              <span className="text-xs text-green-400 font-mono">OK</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
