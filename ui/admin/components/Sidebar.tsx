'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

export default function Sidebar() {
    const pathname = usePathname();

    const links = [
        { href: '/', label: 'Overview', icon: 'M3 3v18h18V3H3zm8 16H5v-6h6v6zm0-8H5V5h6v6zm8 8h-6v-6h6v6zm0-8h-6V5h6v6z' },
        { href: '/data', label: 'Data Browser', icon: 'M20 6h-8l-2-2H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm0 12H4V8h16v10z' },
        { href: '/graphiql', label: 'GraphiQL', icon: 'M16 6l2.29 2.29-4.88 4.88-4-4L2 16.59 3.41 18l6-6 4 4 6.3-6.29L22 12V6z' }, // Activity icon reused for now
        { href: '/api-docs', label: 'REST API', icon: 'M20 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm-5 14H9v-2h6v2zm0-5H9v-2h6v2zm0-5H9V6h6v2z' },
        { href: '/settings', label: 'Settings', icon: 'M19.14 12.94c.04-.3.06-.61.06-.94 0-.32-.02-.64-.07-.94l2.03-1.58a.49.49 0 0 0 .12-.61l-1.92-3.32a.488.488 0 0 0-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94l-.36-2.54a.484.484 0 0 0-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96c-.22-.08-.47 0-.59.22L2.74 8.87c-.12.21-.08.47.12.61l2.03 1.58c-.05.3-.09.63-.09.94s.02.64.07.94l-2.03 1.58a.49.49 0 0 0-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.58 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32c.12-.22.07-.47-.12-.61l-2.01-1.58zM12 15.6c-1.98 0-3.6-1.62-3.6-3.6s1.62-3.6 3.6-3.6 3.6 1.62 3.6 3.6-1.62 3.6-3.6 3.6z' },
    ];

    return (
        <div className="w-64 h-screen bg-black border-r border-gray-800 flex flex-col">
            <div className="p-6 flex items-center space-x-2">
                <div className="w-8 h-8 bg-blue-600 rounded-lg"></div>
                <h1 className="text-xl font-bold text-white tracking-widest">LUMADB</h1>
            </div>
            <nav className="flex-1 px-4 space-y-1">
                {links.map((link) => {
                    const isActive = pathname === link.href || (link.href !== '/' && pathname.startsWith(link.href));
                    return (
                        <Link
                            key={link.href}
                            href={link.href}
                            className={`flex items-center px-4 py-3 rounded-lg transition-colors ${isActive
                                ? 'bg-blue-600 text-white'
                                : 'text-gray-400 hover:text-white hover:bg-gray-900'
                                }`}
                        >
                            <svg className="w-5 h-5 mr-3 fill-current opacity-80" viewBox="0 0 24 24">
                                <path d={link.icon} />
                            </svg>
                            <span className="font-medium text-sm">{link.label}</span>
                        </Link>
                    );
                })}
            </nav>
            <div className="p-4 border-t border-gray-800 bg-gray-900/50">
                <div className="flex items-center space-x-3">
                    <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse"></div>
                    <span className="text-xs text-gray-400 font-mono">v2.0.0-alpha</span>
                </div>
            </div>
        </div>
    );
}
