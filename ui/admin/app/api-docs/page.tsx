'use client';

import { useState } from 'react';

export default function ApiDocs() {
    const [activeTab, setActiveTab] = useState('endpoints');

    // content as placeholder for real generated docs
    const endpoints = [
        { method: 'GET', path: '/api/v1/users', desc: 'List all users', params: '?limit=10&offset=0' },
        { method: 'POST', path: '/api/v1/users', desc: 'Create a new user', params: 'JSON Body' },
        { method: 'GET', path: '/api/v1/users/:id', desc: 'Get user by ID', params: '' },
        { method: 'PUT', path: '/api/v1/users/:id', desc: 'Update user', params: 'JSON Body' },
        { method: 'DELETE', path: '/api/v1/users/:id', desc: 'Delete user', params: '' },
        { method: 'GET', path: '/api/v1/orders', desc: 'List all orders', params: '?limit=10' },
    ];

    return (
        <div className="space-y-6">
            <div className="flex justify-between items-center">
                <div>
                    <h2 className="text-3xl font-bold text-white tracking-tight">REST API Documentation</h2>
                    <p className="text-gray-400 mt-1">Auto-generated endpoints for your collections.</p>
                </div>
                <div className="flex space-x-2">
                    <span className="px-3 py-1 bg-green-500/10 text-green-400 border border-green-500/20 rounded-full text-xs font-mono">OpenAPI 3.0</span>
                    <span className="px-3 py-1 bg-blue-500/10 text-blue-400 border border-blue-500/20 rounded-full text-xs font-mono">v1.0</span>
                </div>
            </div>

            <div className="bg-gray-900/50 backdrop-blur-xl border border-gray-800/50 rounded-2xl overflow-hidden">
                <div className="border-b border-gray-800 flex">
                    <button
                        onClick={() => setActiveTab('endpoints')}
                        className={`px-6 py-4 text-sm font-medium transition-colors ${activeTab === 'endpoints' ? 'text-blue-400 border-b-2 border-blue-400 bg-blue-400/5' : 'text-gray-400 hover:text-white'}`}
                    >
                        Endpoints
                    </button>
                    <button
                        onClick={() => setActiveTab('schemas')}
                        className={`px-6 py-4 text-sm font-medium transition-colors ${activeTab === 'schemas' ? 'text-blue-400 border-b-2 border-blue-400 bg-blue-400/5' : 'text-gray-400 hover:text-white'}`}
                    >
                        Schemas
                    </button>
                </div>

                <div className="p-6">
                    {activeTab === 'endpoints' && (
                        <div className="space-y-4">
                            {endpoints.map((ep, i) => (
                                <div key={i} className="group flex items-start p-4 rounded-xl border border-gray-800 hover:border-gray-700 hover:bg-gray-800/30 transition-all cursor-pointer">
                                    <div className={`w-20 font-bold text-xs uppercase py-1 px-2 rounded text-center mr-4 
                                ${ep.method === 'GET' ? 'bg-blue-500/20 text-blue-400' :
                                            ep.method === 'POST' ? 'bg-green-500/20 text-green-400' :
                                                ep.method === 'DELETE' ? 'bg-red-500/20 text-red-400' : 'bg-orange-500/20 text-orange-400'
                                        }`}>
                                        {ep.method}
                                    </div>
                                    <div className="flex-1">
                                        <div className="flex items-center space-x-3 mb-1">
                                            <code className="text-sm text-gray-200 font-mono">{ep.path}</code>
                                            <span className="text-gray-500 text-xs">- {ep.desc}</span>
                                        </div>
                                        {ep.params && <div className="text-xs text-gray-500 font-mono mt-1">Params: {ep.params}</div>}
                                    </div>
                                    <div className="text-gray-600 group-hover:text-blue-400 transition-colors">
                                        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M9 5l7 7-7 7"></path></svg>
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                    {activeTab === 'schemas' && (
                        <div className="text-center py-10 text-gray-500">
                            Schemas functionality not implemented in mock.
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
