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

    # NEW ENHANCED METHODS FOR BETTER MEMORY MANAGEMENT

    def get_formatted_conversation_history(self, session_id: str, database: str, max_messages: int = 10) -> Dict:
        """Get formatted conversation history for frontend display"""
        history = self.get_conversation_history(session_id, database, max_messages)
        
        formatted_history = []
        total_queries = 0
        total_rows = 0
        
        for i, message in enumerate(history):
            if message['role'] == 'user':
                # This is a user query
                history_item = {
                    'type': 'query',
                    'timestamp': message['timestamp'],
                    'content': message['content'],
                    'sql_query': None,
                    'results': None,
                    'message_id': i
                }
                
                # Look ahead for assistant response
                if i + 1 < len(history) and history[i + 1]['role'] == 'assistant':
                    assistant_msg = history[i + 1]
                    history_item['sql_query'] = assistant_msg.get('sql_query')
                    if assistant_msg.get('results_summary'):
                        results = assistant_msg['results_summary']
                        history_item['results'] = {
                            'row_count': results.get('row_count', 0),
                            'columns': results.get('columns', []),
                            'execution_time': results.get('execution_time', 0)
                        }
                        total_queries += 1
                        total_rows += results.get('row_count', 0)
                
                formatted_history.append(history_item)
        
        return {
            'conversations': formatted_history,
            'stats': {
                'total_queries': total_queries,
                'total_rows_processed': total_rows,
                'session_duration': self._calculate_session_duration(history)
            }
        }

    def _calculate_session_duration(self, history: List[Dict]) -> str:
        """Calculate session duration from first to last message"""
        if not history:
            return "0 minutes"
        
        try:
            first_time = datetime.fromisoformat(history[0]['timestamp'])
            last_time = datetime.fromisoformat(history[-1]['timestamp'])
            duration = last_time - first_time
            minutes = duration.total_seconds() / 60
            
            if minutes < 1:
                return "Less than 1 minute"
            elif minutes < 60:
                return f"{int(minutes)} minutes"
            else:
                hours = minutes / 60
                return f"{hours:.1f} hours"
        except:
            return "Unknown duration"

    def get_conversation_insights(self, session_id: str, database: str) -> Dict:
        """Get insights about the conversation patterns"""
        history = self.get_conversation_history(session_id, database)
        
        # Analyze query patterns
        table_usage = {}
        common_filters = {}
        query_types = {
            'SELECT': 0,
            'COUNT': 0,
            'AGGREGATE': 0,
            'FILTER': 0
        }
        
        for message in history:
            if message.get('sql_query'):
                sql = message['sql_query'].upper()
                
                # Count query types
                if 'COUNT(' in sql:
                    query_types['COUNT'] += 1
                if any(agg in sql for agg in ['SUM(', 'AVG(', 'MAX(', 'MIN(']):
                    query_types['AGGREGATE'] += 1
                if 'WHERE' in sql:
                    query_types['FILTER'] += 1
                query_types['SELECT'] += 1
                
                # Extract table usage
                if 'FROM' in sql:
                    table_part = sql.split('FROM')[1].split()[0].strip('`')
                    table_usage[table_part] = table_usage.get(table_part, 0) + 1
        
        return {
            'query_patterns': query_types,
            'table_usage': table_usage,
            'total_interactions': len([m for m in history if m['role'] == 'user']),
            'most_active_period': self._get_most_active_period(history)
        }

    def _get_most_active_period(self, history: List[Dict]) -> str:
        """Determine the most active period in the conversation"""
        if not history:
            return "No activity"
        
        try:
            hours = []
            for message in history:
                dt = datetime.fromisoformat(message['timestamp'])
                hours.append(dt.hour)
            
            if hours:
                avg_hour = sum(hours) / len(hours)
                if 5 <= avg_hour < 12:
                    return "Morning"
                elif 12 <= avg_hour < 17:
                    return "Afternoon"
                elif 17 <= avg_hour < 22:
                    return "Evening"
                else:
                    return "Night"
        except:
            pass
        
        return "Various times"

# Global memory manager instance
memory_manager = MemoryManager()
