const tagsBar = document.getElementById('tags-bar');
const inputBar = document.getElementById('input-bar');
const imageCanvas = document.getElementById('image-canvas');

let tags = JSON.parse(localStorage.getItem('tags') || '[]');
let imagePath = '';

tagsBar.value = joinTags();
inputBar.value = imagePath;

inputBar.onpaste = ()=>{
  const items = (event.clipboardData || event.originalEvent.clipboardData).items;
  // console.log(items); // will give you the mime types
  for (index in items) {
    const item = items[index];
    if (item.kind === 'file') {
      const file = item.getAsFile();

      let reader = new FileReader();
      reader.onload = function(event) {
        const extension = file.type.match(/\/([a-z0-9]+)/i)[1].toLowerCase();

        let formData = new FormData();
        formData.append('file', file, file.name);
        formData.append('extension', extension);
        formData.append('mimetype', file.type);
        formData.append('submission-type', 'paste');
        // formData.append('imagePath', imagePath);
        formData.append('tags', tags);

        localStorage.setItem('tags', JSON.stringify(tags));

        fetch('/api/images/create', {
          method: 'POST',
          body: formData
        }).then(response=>response.json())
          .then(responseJson=>{
            if(responseJson.filename){
              inputBar.value = responseJson.filename;
              imageCanvas.src = '/images?filename=' + encodeURIComponent(responseJson.trueFilename);
            } else {
              alert(responseJson.message);
            }
          })
          .catch(error => {
            console.error(error);
          });
      };
      reader.readAsBinaryString(file);
    }
  }
}

tagsBar.addEventListener("keyup", function(event) {
  function purge(tag){
    tag = tag.trim();
    if(tag){
      tags.push(tag);
    }
  }

  tags = [];
  let purgable = true;
  let currentTag = ''

  tagsBar.value.split('').forEach((character, index)=>{
    if(character === ',' && purgable){
      if(purgable){
        purge(currentTag);
        currentTag = '';
      } else {
        currentTag += character;
      }
    } else if (character === '\"'){
      purgable = !purgable;
    } else {
      currentTag += character;
    }
  });

  purge(currentTag);
});

function joinTags(){
  let result = []
  tags.forEach((tag, index)=>{
    if(tag.indexOf(',') !== -1){
      result.push('\"' + tag + '\"');
    } else {
      result.push(tag);
    }
  });

  return result.join(', ')
}