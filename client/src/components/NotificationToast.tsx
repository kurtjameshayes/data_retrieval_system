import { useNotificationStore, type Notification } from "@/lib/store";
import { X, Loader2, CheckCircle2, XCircle, Info } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";

const iconMap = {
  loading: Loader2,
  success: CheckCircle2,
  error: XCircle,
  info: Info,
};

const colorMap = {
  loading: "border-blue-500/50 bg-blue-500/10",
  success: "border-green-500/50 bg-green-500/10",
  error: "border-red-500/50 bg-red-500/10",
  info: "border-neutral-500/50 bg-neutral-500/10",
};

const iconColorMap = {
  loading: "text-blue-400",
  success: "text-green-400",
  error: "text-red-400",
  info: "text-neutral-400",
};

function NotificationItem({ notification }: { notification: Notification }) {
  const { removeNotification } = useNotificationStore();
  const Icon = iconMap[notification.type];

  return (
    <motion.div
      initial={{ opacity: 0, x: 100, scale: 0.95 }}
      animate={{ opacity: 1, x: 0, scale: 1 }}
      exit={{ opacity: 0, x: 100, scale: 0.95 }}
      transition={{ duration: 0.2 }}
      className={`relative flex items-start gap-3 p-4 rounded-lg border ${colorMap[notification.type]} backdrop-blur-sm shadow-lg max-w-sm`}
      data-testid={`notification-${notification.id}`}
    >
      <div className={`flex-shrink-0 ${iconColorMap[notification.type]}`}>
        <Icon 
          className={`h-5 w-5 ${notification.type === 'loading' ? 'animate-spin' : ''}`} 
        />
      </div>
      
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-foreground">
          {notification.title}
        </p>
        {notification.message && (
          <p className="mt-1 text-xs text-muted-foreground line-clamp-2">
            {notification.message}
          </p>
        )}
        {notification.queryName && (
          <p className="mt-1 text-xs font-mono text-muted-foreground truncate">
            {notification.queryName}
          </p>
        )}
      </div>

      <button
        onClick={() => removeNotification(notification.id)}
        className="flex-shrink-0 text-muted-foreground hover:text-foreground transition-colors"
        data-testid={`notification-dismiss-${notification.id}`}
      >
        <X className="h-4 w-4" />
      </button>
    </motion.div>
  );
}

export default function NotificationToast() {
  const { notifications } = useNotificationStore();

  return (
    <div 
      className="fixed bottom-4 right-4 z-50 flex flex-col gap-2"
      data-testid="notification-container"
    >
      <AnimatePresence mode="popLayout">
        {notifications.map((notification) => (
          <NotificationItem key={notification.id} notification={notification} />
        ))}
      </AnimatePresence>
    </div>
  );
}
