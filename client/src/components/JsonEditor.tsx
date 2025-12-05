import { useState, useEffect } from "react";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { CheckCircle2, XCircle, Code } from "lucide-react";

interface JsonEditorProps {
  value: any;
  onChange: (value: any) => void;
  className?: string;
}

export default function JsonEditor({ value, onChange, className }: JsonEditorProps) {
  const [text, setText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isValid, setIsValid] = useState(true);

  useEffect(() => {
    setText(JSON.stringify(value, null, 2));
  }, [value]);

  const handleChange = (newText: string) => {
    setText(newText);
    try {
      const parsed = JSON.parse(newText);
      setError(null);
      setIsValid(true);
      onChange(parsed);
    } catch (e) {
      setError((e as Error).message);
      setIsValid(false);
    }
  };

  const formatJson = () => {
    try {
      const parsed = JSON.parse(text);
      setText(JSON.stringify(parsed, null, 2));
      setError(null);
      setIsValid(true);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div className={className}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2 text-xs">
          <Code className="h-3 w-3" />
          <span className="font-mono">JSON Editor</span>
        </div>
        <div className="flex items-center gap-2">
          {isValid ? (
            <span className="flex items-center gap-1 text-xs text-green-500">
              <CheckCircle2 className="h-3 w-3" />
              Valid JSON
            </span>
          ) : (
            <span className="flex items-center gap-1 text-xs text-red-500">
              <XCircle className="h-3 w-3" />
              Invalid
            </span>
          )}
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={formatJson}
            className="h-6 text-xs"
          >
            Format
          </Button>
        </div>
      </div>
      <Textarea
        value={text}
        onChange={(e) => handleChange(e.target.value)}
        className="font-mono text-xs min-h-[300px] bg-muted/50"
        data-testid="json-editor-textarea"
      />
      {error && (
        <p className="text-xs text-red-500 mt-1 font-mono">{error}</p>
      )}
    </div>
  );
}
