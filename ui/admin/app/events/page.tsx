export default function EventsPage() {
    const triggers = [
        { name: "SendWelcomeEmail", collection: "users", event: "INSERT", sink: "WEBHOOK", active: true },
        { name: "AuditLog", collection: "*", event: "ALL", sink: "REDPANDA", active: true },
    ];

    return (
        <div>
            <div className="flex justify-between items-center mb-8">
                <h2 className="text-3xl font-bold">Event Triggers</h2>
                <button className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors">
                    Create Trigger
                </button>
            </div>

            <div className="grid gap-6">
                {triggers.map((trigger) => (
                    <div key={trigger.name} className="bg-gray-900 p-6 rounded-xl border border-gray-800 flex justify-between items-center group hover:border-gray-700 transition-all">
                        <div>
                            <div className="flex items-center space-x-3 mb-2">
                                <h3 className="font-bold text-lg">{trigger.name}</h3>
                                <span className={`px-2 py-0.5 rounded text-xs font-medium ${trigger.active ? "bg-green-500/20 text-green-400" : "bg-gray-700 text-gray-400"
                                    }`}>
                                    {trigger.active ? "Active" : "Paused"}
                                </span>
                            </div>
                            <div className="text-sm text-gray-400 space-x-4">
                                <span>Collection: <span className="text-white">{trigger.collection}</span></span>
                                <span>Event: <span className="text-blue-400">{trigger.event}</span></span>
                                <span>Sink: <span className="text-purple-400">{trigger.sink}</span></span>
                            </div>
                        </div>
                        <div className="flex space-x-2 opacity-0 group-hover:opacity-100 transition-opacity">
                            <button className="px-3 py-1.5 bg-gray-800 hover:bg-gray-700 rounded text-sm">Edit</button>
                            <button className="px-3 py-1.5 bg-gray-800 hover:bg-red-900/50 text-red-400 rounded text-sm">Delete</button>
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}
