# llm/memory_manager.py
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import hashlib

logger = logging.getLogger(__name__)

class MemoryManager:
    def __init__(self, max_conversations=10, max_messages_per_conversation=20):
        self.memory_store = {}
        self.max_conversations = max_conversations
        self.max_messages_per_conversation = max_messages_per_conversation
    
    def _get_conversation_key(self, session_id: str, database: str) -> str:
        """Generate a unique key for conversation"""
        key_string = f"{session_id}_{database}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def add_message(self, session_id: str, database: str, role: str, content: str, 
                   sql_query: str = None, results_summary: Dict = None):
        """Add a message to conversation history"""
        conv_key = self._get_conversation_key(session_id, database)
        
        if conv_key not in self.memory_store:
            self.memory_store[conv_key] = {
                'session_id': session_id,
                'database': database,
                'messages': [],
                'created_at': datetime.utcnow().isoformat(),
                'last_accessed': datetime.utcnow().isoformat()
            }
        
        message = {
            'role': role,
            'content': content,
            'timestamp': datetime.utcnow().isoformat(),
            'sql_query': sql_query,
            'results_summary': results_summary
        }
        
        self.memory_store[conv_key]['messages'].append(message)
        self.memory_store[conv_key]['last_accessed'] = datetime.utcnow().isoformat()
        
        # Limit messages per conversation
        if len(self.memory_store[conv_key]['messages']) > self.max_messages_per_conversation:
            self.memory_store[conv_key]['messages'] = self.memory_store[conv_key]['messages'][-self.max_messages_per_conversation:]
        
        # Clean up old conversations
        self._cleanup_old_conversations()
        
        logger.info(f"Added message to conversation {conv_key}. Total messages: {len(self.memory_store[conv_key]['messages'])}")
    
    def get_conversation_history(self, session_id: str, database: str, max_messages: int = 10) -> List[Dict]:
        """Get recent conversation history"""
        conv_key = self._get_conversation_key(session_id, database)
        
        if conv_key not in self.memory_store:
            return []
        
        # Update last accessed time
        self.memory_store[conv_key]['last_accessed'] = datetime.utcnow().isoformat()
        
        # Return recent messages (limit to max_messages)
        messages = self.memory_store[conv_key]['messages']
        return messages[-max_messages:] if max_messages else messages
    
    def get_conversation_summary(self, session_id: str, database: str) -> str:
        """Get a natural language summary of the conversation"""
        history = self.get_conversation_history(session_id, database, max_messages=5)
        
        if not history:
            return "No previous conversation history."
        
        summary = "Previous conversation context:\n"
        
        for i, message in enumerate(history[-3:], 1):  # Last 3 messages
            if message['role'] == 'user':
                summary += f"User asked: {message['content']}\n"
                if message.get('sql_query'):
                    summary += f"SQL used: {message['sql_query']}\n"
            elif message['role'] == 'assistant':
                if message.get('results_summary'):
                    results = message['results_summary']
                    summary += f"Found {results.get('row_count', 0)} rows with columns: {', '.join(results.get('columns', []))}\n"
        
        return summary
    
    def get_schema_learning(self, session_id: str, database: str) -> Dict[str, Any]:
        """Extract learned schema patterns from conversation history"""
        history = self.get_conversation_history(session_id, database)
        
        learned_tables = set()
        learned_columns = {}
        common_filters = {}
        
        for message in history:
            if message.get('sql_query'):
                sql = message['sql_query'].lower()
                
                # Extract table names (simple pattern matching)
                if 'from' in sql:
                    # Simple table extraction - can be enhanced
                    parts = sql.split('from')
                    if len(parts) > 1:
                        table_part = parts[1].split()[0].strip('`')
                        if table_part and len(table_part) < 50:  # Basic validation
                            learned_tables.add(table_part)
                
                # Extract column patterns from user queries
                if message['role'] == 'user':
                    content_lower = message['content'].lower()
                    # Look for column mentions
                    column_indicators = ['column', 'field', 'show me', 'find', 'filter by', 'where']
                    for indicator in column_indicators:
                        if indicator in content_lower:
                            # Simple extraction - can be enhanced with NLP
                            pass
        
        return {
            'tables': list(learned_tables),
            'columns': learned_columns,
            'common_filters': common_filters
        }
    
    def _cleanup_old_conversations(self):
        """Remove old conversations to manage memory"""
        if len(self.memory_store) <= self.max_conversations:
            return
        
        # Sort by last accessed time and remove oldest
        conversations = sorted(
            self.memory_store.items(),
            key=lambda x: x[1]['last_accessed']
        )
        
        # Remove oldest conversations until we're under the limit
        while len(self.memory_store) > self.max_conversations:
            oldest_key = conversations.pop(0)[0]
            del self.memory_store[oldest_key]
            logger.info(f"Cleaned up old conversation: {oldest_key}")
    
    def clear_conversation(self, session_id: str, database: str):
        """Clear specific conversation history"""
        conv_key = self._get_conversation_key(session_id, database)
        if conv_key in self.memory_store:
            del self.memory_store[conv_key]
            logger.info(f"Cleared conversation: {conv_key}")
    
    def clear_all_conversations(self):
        """Clear all conversation history"""
        self.memory_store.clear()
        logger.info("Cleared all conversation history")

# Global memory manager instance
memory_manager = MemoryManager()
