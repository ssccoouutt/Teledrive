def apply_formatting(text, entities):
    """Apply all formatting with proper nesting and blockquote support"""
    if not text:
        return text

    # First handle blockquotes (lines starting with >)
    if ">" in text:
        text = text.replace("&gt;", ">")  # Unescape HTML entities first
        lines = text.split('\n')
        formatted_lines = []
        in_blockquote = False
        
        for line in lines:
            stripped_line = line.lstrip()
            if stripped_line.startswith('>'):
                if not in_blockquote:
                    formatted_lines.append('<blockquote>')
                    in_blockquote = True
                # Preserve original indentation before the >
                indent = line[:line.index('>')]
                content = line[line.index('>')+1:].strip()
                formatted_lines.append(f"{indent}{content}")
            else:
                if in_blockquote:
                    formatted_lines.append('</blockquote>')
                    in_blockquote = False
                formatted_lines.append(line)
        
        if in_blockquote:
            formatted_lines.append('</blockquote>')
        
        text = '\n'.join(formatted_lines)

    # Convert to list for character-level manipulation of other entities
    chars = list(text)
    text_length = len(chars)
    
    # Sort entities by offset (reversed for proper insertion)
    sorted_entities = sorted(entities or [], key=lambda e: -e.offset)
    
    # Entity processing map
    entity_tags = {
        MessageEntity.BOLD: ('<b>', '</b>'),
        MessageEntity.ITALIC: ('<i>', '</i>'),
        MessageEntity.UNDERLINE: ('<u>', '</u>'),
        MessageEntity.STRIKETHROUGH: ('<s>', '</s>'),
        MessageEntity.SPOILER: ('<tg-spoiler>', '</tg-spoiler>'),
        MessageEntity.CODE: ('<code>', '</code>'),
        MessageEntity.PRE: ('<pre>', '</pre>'),
        MessageEntity.TEXT_LINK: (lambda e: f'<a href="{e.url}">', '</a>')
    }
    
    for entity in sorted_entities:
        entity_type = entity.type
        if entity_type not in entity_tags:
            continue
            
        start_tag, end_tag = entity_tags[entity_type]
        if callable(start_tag):
            start_tag = start_tag(entity)
            
        start = entity.offset
        end = start + entity.length
        
        # Validate positions
        if start >= text_length or end > text_length:
            continue
            
        # Apply formatting
        before = ''.join(chars[:start])
        content = ''.join(chars[start:end])
        after = ''.join(chars[end:])
        
        # Special handling for blockquotes to prevent nesting issues
        if '<blockquote>' in content or '</blockquote>' in content:
            content = content.replace('<blockquote>', '').replace('</blockquote>', '')
        
        chars = list(before + start_tag + content + end_tag + after)
        text_length = len(chars)
    
    # Final HTML escaping (except for our tags)
    formatted_text = ''.join(chars)
    formatted_text = formatted_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    
    # Re-insert our HTML tags
    html_tags = ['b', 'i', 'u', 's', 'code', 'pre', 'a', 'tg-spoiler', 'blockquote']
    for tag in html_tags:
        formatted_text = formatted_text.replace(f'&lt;{tag}&gt;', f'<{tag}>').replace(f'&lt;/{tag}&gt;', f'</{tag}>')
    
    return formatted_text
